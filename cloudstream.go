/*
Cloudstream is a command to stream files from/to Google Cloud Storage, e.g. for reading and writing backups.

For now, the command "cloudstream" can only read and write files.
It has no special knowledge about buckets, and no abilities to
list directories or remove files.

To use, first you must create a configuration file called
"cloudstream.conf", in the current working directory or in a directory
higher up:

	accesskey ABCDEF0123456789
	secret long-secret-provided-by-google

You can find these parameters in the Google API's Console, under
"Google Cloud Storage", under "Interopable Access".

Now you can write a file:
	echo 'hi there!' | cloudstream put /mybucket/greeting.txt

And you can read it back again:
	cloudstream get /mybucket/greeting.txt

This package uses the simple REST API from Amazon S3, but on Google
Cloud Storage.  This keeps authentication bearable, and means the
ugly automatically generated JSON-based API doesn't have to be used.
Note that this library won't work on AWS S3.  S3 doesn't support
streaming uploads with the "chunked" transfer-encoding.  To "stream"
to S3, you have to fake it by uploading 5MB chunks of file.  Making
it all a bit inconvenient to get decent transfer rates.
*/
package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"bitbucket.org/mjl/tokenize"
)

var config struct {
	AccessKey string // AWS/Google access key, identifying account
	Secret    string // For signing requests
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: cloudstream [put file | get file]\n")
	os.Exit(2)
}

func fail(s string) {
	fmt.Fprintln(os.Stderr, s)
	os.Exit(1)
}

// looks for config file in current directory, then directories higher up
func findconfig(p, name string) string {
	var err error
	if p != "" {
		p, err = os.Getwd()
		if err != nil {
			fail(fmt.Sprintf("finding %s: %s", name, err))
		}
	}
	for {
		pp := p + "/" + name
		if _, err = os.Stat(pp); !os.IsNotExist(err) {
			return pp
		}
		np := path.Dir(p)
		if np == p {
			break
		}
		p = np
	}
	fail("could not find " + name)
	panic("")
}

func parseconfig(p string) {
	lines, err := tokenize.File(p)
	if err != nil {
	}
	for _, l := range lines {
		cmd, l := l[0], l[1:]
		need := func(n int) {
			if n != len(l) {
				fail(fmt.Sprintf("bad parameters for %q, expected %d, saw %d", cmd, n, len(l)))
			}
		}
		switch cmd {
		case "accesskey":
			need(1)
			config.AccessKey = l[0]
		case "secret":
			need(1)
			config.Secret = l[0]
		default:
			fail(fmt.Sprintf("bad config command %q", cmd))
		}
	}
}

// Make HTTP authorization header for AWS-style authentication.
func authorize(msg string) string {
	h := hmac.New(sha1.New, []byte(config.Secret))
	h.Write([]byte(msg))
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("AWS %s:%s", config.AccessKey, sig)
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	parseconfig(findconfig("", "cloudstream.conf"))

	makepath := func(path string) string {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		return path
	}

	writeresponse := func(resp *http.Response) {
		out := os.Stdout
		if resp.StatusCode != 200 {
			out = os.Stderr
		}

		defer resp.Body.Close()
		_, err := io.Copy(out, resp.Body)
		if resp.StatusCode != 200 {
			fail("status: " + resp.Status)
		}
		if err != nil {
			fail(err.Error())
		}
	}

	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	default:
		usage()

	case "get":
		if len(args) != 1 {
			usage()
		}
		path := makepath(args[0])
		client := new(http.Client)
		req, err := http.NewRequest("GET", "https://storage.googleapis.com"+path, nil)
		if err != nil {
			fail(err.Error())
		}

		date := time.Now().Format(time.RFC1123Z)
		req.Header.Add("Date", date)

		msg := "GET\n"
		msg += "\n"
		msg += "\n"
		msg += date + "\n"
		msg += path

		req.Header.Add("Authorization", authorize(msg))

		resp, err := client.Do(req)
		if err != nil {
			fail(err.Error())
		}
		writeresponse(resp)

	case "put":
		if len(args) != 1 {
			usage()
		}

		path := makepath(args[0])

		client := new(http.Client)
		req, err := http.NewRequest("PUT", "https://storage.googleapis.com"+path, nil)
		if err != nil {
			fail(err.Error())
		}

		date := time.Now().Format(time.RFC1123Z)
		req.Header.Add("Date", date)

		msg := "PUT\n"
		msg += "\n"
		msg += "\n"
		msg += date + "\n"
		msg += path

		req.Header.Add("Authorization", authorize(msg))

		req.ContentLength = 0
		pr, pw := io.Pipe()
		req.Body = pr
		go func() {
			_, err := io.Copy(pw, os.Stdin)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			err = pw.Close()
			if err != nil {
				fail(err.Error())
			}
		}()

		resp, err := client.Do(req)
		if err != nil {
			fail(err.Error())
		}
		writeresponse(resp)
	}
}
