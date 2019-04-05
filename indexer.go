// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package n1fty

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"

	"gopkg.in/couchbase/gocbcore.v7"
)

// FTSIndexer implements datastore.Indexer interface
type FTSIndexer struct {
	namespace string
	keyspace  string
	serverURL string

	agent *gocbcore.Agent

	cfg     cbgt.Cfg
	stats   *stats
	closeCh chan struct{}

	// sync RWMutex protects following fields
	m sync.RWMutex

	client   *ftsClient
	nodeDefs *cbgt.NodeDefs

	indexIds   []string
	indexNames []string
	allIndexes []datastore.Index

	mapIndexesByID   map[string]datastore.Index
	mapIndexesByName map[string]datastore.Index
}

type stats struct {
	TotalSearch                int64
	TotalSearchDuration        int64
	TotalTTFBDuration          int64 // time to first response byte
	TotalThrottledFtsDuration  int64
	TotalThrottledN1QLDuration int64
	TotalBackFills             int64
	CurBackFillSize            int64
}

// -----------------------------------------------------------------------------

func NewFTSIndexer(serverIn, namespace, keyspace string) (datastore.Indexer,
	errors.Error) {
	logging.Infof("n1fty: server: %v, namespace: %v, keyspace: %v",
		serverIn, namespace, keyspace)

	server, _, bucketName :=
		cbgt.CouchbaseParseSourceName(serverIn, "default", keyspace)

	conf := &gocbcore.AgentConfig{
		UserString:           "n1fty",
		BucketName:           bucketName,
		ConnectTimeout:       60000 * time.Millisecond, // TODO: configurability.
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		Auth:                 &Authenticator{},
	}

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, util.N1QLError(fmt.Errorf(
			"NewFTSIndexer, no servers provided"), "")
	}

	err := conf.FromConnStr(svrs[0])
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	agent, err := gocbcore.CreateAgent(conf)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	if ftsConfig == nil {
		initConfig()
	}

	indexer := &FTSIndexer{
		namespace: namespace,
		keyspace:  keyspace,
		serverURL: svrs[0],
		agent:     agent,
		cfg:       ftsConfig,
		stats:     &stats{},
		closeCh:   make(chan struct{}),
	}

	go backfillMonitor(1*time.Second, indexer) // TODO: configurability.

	go logStats(60*time.Second, indexer) // TODO: configurability.

	go cfgListener(indexer)

	return indexer, nil
}

func cfgListener(i *FTSIndexer) {
	ec := make(chan cbgt.CfgEvent)
	i.cfg.Subscribe(cbgt.INDEX_DEFS_KEY, ec)
	i.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN), ec)
	for {
		select {
		case <-i.closeCh:
			return
		case _ = <-ec:
			i.Refresh()
		}
	}
}

type Authenticator struct{}

func (a *Authenticator) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(
		endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocbcore.UserPassPair{{}}, err
	}

	return []gocbcore.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func (i *FTSIndexer) SetConnectionSecurityConfig(
	conf *datastore.ConnectionSecurityConfig) {
	if conf == nil {
		return
	}

	newSecuritySetting := &securitySetting{
		tlsPreference:     &conf.TLSConfig,
		encryptionEnabled: conf.ClusterEncryptionConfig.EncryptData,
		disableNonSSLPort: conf.ClusterEncryptionConfig.DisableNonSSLPorts,
	}

	if len(conf.CertFile) != 0 && len(conf.KeyFile) != 0 {
		certificate, err := tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			logging.Fatalf("Failed to generate SSL certificate, err: %v", err)
		}
		newSecuritySetting.certificate = &certificate

		newSecuritySetting.certInBytes, err = ioutil.ReadFile(conf.CertFile)
		if err != nil {
			logging.Fatalf("Failed to load certificate file, err: %v", err)
		}
	}

	updateSecuritySetting(newSecuritySetting)

	i.Refresh()
}

// Close is an implementation of io.Closer interface
// It is recommended that query calls Close on the FTSIndexer
// object once its usage is over, for a graceful cleanup.
func (i *FTSIndexer) Close() error {
	close(i.closeCh)
	return nil
}

// SetCfg for better testing
func (i *FTSIndexer) SetCfg(cfg cbgt.Cfg) {
	i.cfg = cfg
}

func (i *FTSIndexer) KeyspaceId() string {
	return i.keyspace
}

func (i *FTSIndexer) Name() datastore.IndexType {
	return datastore.FTS
}

func (i *FTSIndexer) IndexIds() ([]string, errors.Error) {
	if err := i.refresh(false); err != nil {
		return nil, err
	}

	i.m.RLock()
	indexIds := i.indexIds
	i.m.RUnlock()

	return indexIds, nil
}

func (i *FTSIndexer) IndexNames() ([]string, errors.Error) {
	if err := i.refresh(false); err != nil {
		return nil, err
	}

	i.m.RLock()
	indexNames := i.indexNames
	i.m.RUnlock()

	return indexNames, nil
}

func (i *FTSIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	// no refresh
	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesByID != nil {
		index, ok := i.mapIndexesByID[id]
		if ok {
			return index, nil
		}
	}

	return nil, util.N1QLError(nil,
		fmt.Sprintf("IndexById, fts index with id: %v not found", id))
}

func (i *FTSIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	// no refresh
	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesByName != nil {
		index, ok := i.mapIndexesByName[name]
		if ok {
			return index, nil
		}
	}

	return nil, util.N1QLError(nil,
		fmt.Sprintf("IndexByName, fts index with name: %v not found", name))
}

func (i *FTSIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return nil, nil
}

func (i *FTSIndexer) Indexes() ([]datastore.Index, errors.Error) {
	if err := i.refresh(false); err != nil {
		return nil, util.N1QLError(err, "")
	}

	i.m.RLock()
	allIndexes := i.allIndexes
	i.m.RUnlock()

	return allIndexes, nil
}

func (i *FTSIndexer) CreatePrimaryIndex(requestId, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, util.N1QLError(nil, "CreatePrimaryIndex not supported")
}

func (i *FTSIndexer) CreateIndex(requestId, name string,
	seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {
	return nil, util.N1QLError(nil, "CreateIndex not supported")
}

func (i *FTSIndexer) BuildIndexes(requestId string, name ...string) errors.Error {
	return util.N1QLError(nil, "BuildIndexes not supported")
}

func (i *FTSIndexer) Refresh() errors.Error {
	return i.refresh(true)
}

func (i *FTSIndexer) MetadataVersion() uint64 {
	// FIXME
	return 0
}

func (i *FTSIndexer) SetLogLevel(level logging.Level) {
	logging.SetLevel(level)
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) refresh(force bool) errors.Error {
	mapIndexesByID, nodeDefs, err := i.refreshConfigs()
	if err != nil {
		return util.N1QLError(err, "refresh failed")
	}

	err = i.initClient(nodeDefs, force)
	if err != nil {
		return util.N1QLError(err, "initClient failed")
	}

	numIndexes := len(mapIndexesByID)
	indexIds := make([]string, 0, numIndexes)
	indexNames := make([]string, 0, numIndexes)
	allIndexes := make([]datastore.Index, 0, numIndexes)

	mapIndexesByName := map[string]datastore.Index{}

	for id, index := range mapIndexesByID {
		indexIds = append(indexIds, id)
		indexNames = append(indexNames, index.Name())
		allIndexes = append(allIndexes, index)
		mapIndexesByName[index.Name()] = index
	}

	i.m.Lock()
	i.indexIds = indexIds
	i.indexNames = indexNames
	i.allIndexes = allIndexes
	i.mapIndexesByID = mapIndexesByID
	i.mapIndexesByName = mapIndexesByName
	i.m.Unlock()

	bmrw, err := i.fetchBleveMaxResultWindow()
	if err == nil && int64(bmrw) != util.GetBleveMaxResultWindow() {
		util.SetBleveMaxResultWindow(int64(bmrw))
	}

	return nil
}

func (i *FTSIndexer) refreshConfigs() (
	map[string]datastore.Index, *cbgt.NodeDefs, error) {
	var cfg cbgt.Cfg
	cfg = ftsConfig
	if i.cfg != nil {
		cfg = i.cfg
	}

	var err error

	// first try to load configs from local config cache
	indexDefs, _ := GetIndexDefs(cfg)
	nodeDefs, _ := GetNodeDefs(cfg)

	// if not available in config, try fetching them from an fts node
	if indexDefs == nil || nodeDefs == nil {
		ftsEndpoints := i.agent.FtsEps()
		if len(ftsEndpoints) == 0 {
			return nil, nil, nil
		}
		now := time.Now().UnixNano()
		indexDefs, nodeDefs, err = i.retrieveIndexDefs(
			ftsEndpoints[now%int64(len(ftsEndpoints))])
		if err != nil {
			return nil, nil, err
		}
	}

	if indexDefs == nil {
		return nil, nil, nil
	}

	if nodeDefs == nil {
		return nil, nil, nil
	}

	imap, err := i.convertIndexDefs(indexDefs)
	if err != nil {
		return nil, nil, err
	}

	imap, err = i.addFlexIndexes(imap)

	return imap, nodeDefs, err
}

func (i *FTSIndexer) nodeDefsUnchangedLOCKED(newNodeDefs *cbgt.NodeDefs) bool {
	oldBytes, err1 := json.Marshal(i.nodeDefs)
	newBytes, err2 := json.Marshal(newNodeDefs)
	if err1 != nil || err2 != nil {
		return false
	}
	var oldI, newI interface{}
	err1 = json.Unmarshal(oldBytes, &oldI)
	err2 = json.Unmarshal(newBytes, &newI)
	if err1 != nil || err2 != nil {
		return false
	}
	return reflect.DeepEqual(oldI, newI)
}

func (i *FTSIndexer) initClient(nodeDefs *cbgt.NodeDefs, force bool) error {
	i.m.RLock()
	if i.client != nil && i.nodeDefsUnchangedLOCKED(nodeDefs) && !force {
		i.m.RUnlock()
		return nil
	}
	i.m.RUnlock()

	i.m.Lock()
	defer i.m.Unlock()
	if i.client != nil && i.nodeDefsUnchangedLOCKED(nodeDefs) && !force {
		return nil
	}

	// setup new client
	var err error
	i.client, err = setupFTSClient(nodeDefs)
	if err != nil {
		return err
	}

	i.nodeDefs = nodeDefs

	return nil
}

func (i *FTSIndexer) getClient() *ftsClient {
	var client *ftsClient
	i.m.RLock()
	client = i.client
	i.m.RUnlock()
	return client
}

func (i *FTSIndexer) retrieveIndexDefs(node string) (
	*cbgt.IndexDefs, *cbgt.NodeDefs, error) {
	httpClient := i.agent.HttpClient()
	if httpClient == nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, client not available")
	}

	cbauthURL, err := cbgt.CBAuthURL(node + "/api/cfg")
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, err: %v", err)
	}

	resp, err := httpClient.Get(cbauthURL)
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, err: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, resp status code: %v",
			resp.StatusCode)
	}

	bodyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, resp body read err: %v", err)
	}

	var body struct {
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
		NodeDefs  *cbgt.NodeDefs  `json:"nodeDefsKnown"`
		Status    string          `json:"status"`
	}

	err = json.Unmarshal(bodyBuf, &body)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieveIndexDefs, json err: %v", err)
	}

	if body.Status != "ok" {
		return nil, nil, fmt.Errorf("retrieveIndexDefs, status code: %s",
			body.Status)
	}

	return body.IndexDefs, body.NodeDefs, nil
}

func (i *FTSIndexer) fetchBleveMaxResultWindow() (int, error) {
	ftsEndpoints := i.agent.FtsEps()
	if len(ftsEndpoints) == 0 {
		return 0, fmt.Errorf("no fts endpoints available")
	}

	now := time.Now().UnixNano()
	cbauthURL, err := cbgt.CBAuthURL(
		ftsEndpoints[now%int64(len(ftsEndpoints))] + "/api/manager")
	if err != nil {
		return 0, err
	}

	httpClient := i.agent.HttpClient()
	if httpClient == nil {
		return 0, fmt.Errorf("client not available")
	}

	resp, err := httpClient.Get(cbauthURL)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status code: %v", resp.StatusCode)
	}

	bodyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var expect map[string]interface{}
	err = json.Unmarshal(bodyBuf, &expect)
	if err != nil {
		return 0, err
	}

	if status, exists := expect["status"]; !exists || status.(string) != "ok" {
		return 0, err
	}

	if mgr, exists := expect["mgr"]; exists {
		mgrMap, _ := mgr.(map[string]interface{})
		options, _ := mgrMap["options"].(map[string]interface{})
		if bleveMaxResultWindow, exists := options["bleveMaxResultWindow"]; exists {
			return strconv.Atoi(bleveMaxResultWindow.(string))
		}
	}

	return 0, fmt.Errorf("value of bleveMaxResultWindow unknown")
}

// Convert FTS index definitions into a map of n1ql index id mapping to
// datastore.FTSIndex
func (i *FTSIndexer) convertIndexDefs(indexDefs *cbgt.IndexDefs) (
	map[string]datastore.Index, error) {
	rv := map[string]datastore.Index{}
	for _, indexDef := range indexDefs.IndexDefs {
		// TODO: Also check the keyspace's UUID (or, bucket's UUID)?
		if indexDef.SourceName != i.keyspace {
			// If the source name of the index definition doesn't
			// match the indexer's keyspace, do not include the
			// index.
			continue
		}
		im, docConfig, searchFields, condExpr, dynamic, defaultAnalyzer,
			defaultDateTimeParser, err := util.ProcessIndexDef(indexDef)
		if err != nil {
			return nil, err
		}

		if len(searchFields) > 0 || dynamic {
			rv[indexDef.UUID], err = newFTSIndex(i, indexDef,
				searchFields, condExpr, dynamic,
				defaultAnalyzer, defaultDateTimeParser)
			if err != nil {
				return nil, err
			}

			// set this index mapping into the indexMappings cache
			util.SetIndexMapping(indexDef.Name, &util.MappingDetails{
				UUID:       indexDef.UUID,
				SourceName: indexDef.SourceName,
				IMapping:   im,
				DocConfig:  docConfig,
			})
		}
	}

	return rv, nil
}
