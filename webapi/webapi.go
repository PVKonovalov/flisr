// Created by Pavel Konovalov pkonovalov@orxagrid.com
//
// The webapi package implements access to the WEB API server
//

package webapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Connection struct {
	Timeout                time.Duration
	BaseUrl                string
	HostVirtualName        string
	Token                  string
	DatastreamResponseTime float64
	DontNotSendData        bool
}

var TokenExpiredError = errors.New("token expired")

// Logon to WEB API server. Return token and error
func (c *Connection) Logon(username string, password string) (string, error, float64) {

	api := http.Client{Timeout: c.Timeout}
	requestUrl := c.BaseUrl + "/api/token"

	credentials := url.Values{}
	credentials.Set("username", username)
	credentials.Set("password", password)

	req, err := http.NewRequest(http.MethodPost, requestUrl, strings.NewReader(credentials.Encode()))
	if err != nil {
		return "", err, -1
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if c.HostVirtualName != "" {
		req.Header.Set("Host", c.HostVirtualName)
	}

	start := time.Now()
	resp, err := api.Do(req)
	responseTime := time.Since(start).Seconds()

	if err != nil {
		return "", err, responseTime
	}

	if resp.StatusCode != 200 {
		_ = resp.Body.Close()
		return "", errors.New(fmt.Sprintf("logon: invalid status (%s) (%d)", resp.Status, resp.StatusCode)), responseTime
	}

	var resultJson map[string]interface{}

	err = json.NewDecoder(resp.Body).Decode(&resultJson)
	_ = resp.Body.Close()

	if err != nil {
		return "", err, responseTime
	}

	if resultJson["access_token"] == nil {
		return "", errors.New("access token is empty"), responseTime
	}

	c.Token = resultJson["access_token"].(string)
	return c.Token, nil, responseTime
}

// GetProfile from WEB API server
func (c *Connection) GetProfile(path string) ([]byte, error) {
	api := http.Client{Timeout: c.Timeout}
	requestUrl := c.BaseUrl + path

	req, err := http.NewRequest(http.MethodGet, requestUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.Token)

	if c.HostVirtualName != "" {
		req.Header.Set("Host", c.HostVirtualName)
	}

	resp, err := api.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		result, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != 200 {
			return nil, errors.New(fmt.Sprintf("get profile: status (%s) (%d)", resp.Status, resp.StatusCode))
		}
		return result, err
	}

	return nil, errors.New("unknown error")
}

// GetRtdbProfile from WEB API server
func (c *Connection) GetRtdbProfile() ([]byte, error) {
	return c.GetProfile("/api/OgGW/Point/")
}

// GetIecProfile from WEB API server
func (c *Connection) GetIecProfile() ([]byte, error) {
	return c.GetProfile("/api/OgGW/ClientIec104Profile/")
}

// GetHisNode from WEB API server
func (c *Connection) GetHisNode(nodeId uint) ([]byte, error) {
	return c.GetProfile(fmt.Sprintf("/api/OgGW/HisNode/%d", nodeId))
}

// PostDatastream post datastream to requestUrl return request body as result error, and
func (c *Connection) PostDatastream(requestUrl string, datastream string) ([]byte, error, float64) {
	api := http.Client{Timeout: c.Timeout}

	req, err := http.NewRequest(http.MethodPost, requestUrl, strings.NewReader(datastream))
	if err != nil {
		return nil, err, -1
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	req.Header.Set("Content-Type", "application/json")

	if c.HostVirtualName != "" {
		req.Header.Set("Host", c.HostVirtualName)
	}

	start := time.Now()

	if c.DontNotSendData == true {
		return []byte("{\"result\": {}, \"statusCode\": 200}"), nil, float64(time.Since(start).Milliseconds())
	}

	resp, err := api.Do(req)

	responseTime := float64(time.Since(start).Milliseconds())

	if err != nil {
		return nil, err, responseTime
	}

	if resp.StatusCode == 404 { // Don't ask me why GAP returns 404 code if the token has expired
		var resultJson map[string]interface{}
		err := json.NewDecoder(resp.Body).Decode(&resultJson)
		_ = resp.Body.Close()

		if err != nil {
			return nil, errors.New(fmt.Sprintf("post datastream: status (%d) JSON error: %v", resp.StatusCode, err)), responseTime
		}

		if int(resultJson["statusCode"].(float64)) == 401 {
			return nil, TokenExpiredError, -1
		}
	} else if resp.StatusCode == 200 {
		bytes, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return bytes, err, responseTime
	}

	return nil, errors.New(fmt.Sprintf("post datastream: status (%s) (%d)", resp.Status, resp.StatusCode)), responseTime
}
