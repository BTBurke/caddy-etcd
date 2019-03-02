// +build mage

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

func init() {
	reqd := []string{
		"go",
		"sed",
	}
	for _, req := range reqd {
		if _, err := exec.LookPath(req); err != nil {
			log.Fatalf("%s is required to build caddy from source with the etcd plugin", req)
		}
	}

	var err error
	tempDir, err = ioutil.TempDir("", "caddy")
	if err != nil {
		log.Fatal(err)
	}
	cwd, err = os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	version, err = sh.Output("go", "list", "-f", "{{.Version}}", "-m", "github.com/mholt/caddy")
	fmt.Printf("Building caddy version %s\n", version)
	if err != nil || version == "" {
		log.Fatalf("could not determine the version of caddy to build: %v", err)
	}
}

// declare any extra plugins here that you want built into Caddy.  The map should be the import
// name of the plugin and the import path.
var plugins = map[string]string{
	"etcd": "github.com/BTBurke/caddy-etcd",
}

// If you plan on compiling with plugins that are not already listed on Caddyserver.com, use this
// to declare where you want them inserted in the plugin list  Sometimes precedence of these plugins matters.
// The key should be the name the plugin you want to add.  The value is the name of the plugin immediately
// preceeding yours.  Example: `"etcd": "git"` will add the etcd plugin after the git entry.
// See the file `caddyserver/httpserver/plugin.go` for the precedence list built into Caddy.
var plugAfter = map[string]string{
	"etcd": "git",
}

// A build step that requires additional params, or platform specific steps for example
func Build() error {
	mg.Deps(cloneCaddy, cloneEtcd)
	fmt.Printf("caddy and etcd cloned to %s\n", tempDir)
	mg.SerialDeps(insertPlugins, generateEtcd, caddyModules, checkBuild)
	os.RemoveAll(tempDir)
	return nil
}

func cloneCaddy() error {
	dir := path.Join(tempDir, "caddy")
	if err := sh.Run("git", "clone", "https://github.com/mholt/caddy", dir); err != nil {
		return errors.Wrap(err, "failed to clone caddy")
	}
	return nil
}

func cloneEtcd() error {
	dir := path.Join(tempDir, "etcd")
	if err := sh.Run("git", "clone", "--depth=1", "https://github.com/etcd-io/etcd", dir); err != nil {
		return errors.Wrap(err, "failed to clone etcd")
	}
	return nil
}

func insertPlugins() error {
	if err := os.Chdir(path.Join(tempDir, "caddy")); err != nil {
		return errors.Wrap(err, "could not change to caddy directory")
	}
	defer os.Chdir(cwd)
	out, err := sh.Output("git", "checkout", "tags/"+version)
	if err != nil || strings.Contains(out, "error") {
		return errors.Wrapf(err, "chould not check out version %s: %s", version, out)
	}
	p := path.Join(tempDir, "caddy/caddy/caddymain/run.go")
	for _, plug := range plugins {
		repl := fmt.Sprintf("/This is where/a _ \"%s\"", plug)
		if err := sh.Run("sed", "-i", repl, p); err != nil {
			return errors.Wrapf(err, "failed to insert plugin %s in run.go", plug)
		}
	}
	p2 := path.Join(tempDir, "caddy/caddyhttp/httpserver/plugin.go")
	for plug, after := range plugAfter {
		repl := fmt.Sprintf("/\"%s\",/a \"%s\",", after, plug)
		if err := sh.Run("sed", "-i", repl, p2); err != nil {
			return errors.Wrapf(err, "failed to insert plugin %s in plugin.go", plug)
		}
	}
	return nil
}

func generateEtcd() error {
	if err := os.Chdir(path.Join(tempDir, "etcd", "client")); err != nil {
		return errors.Wrap(err, "could not change to etcd to regenerate")
	}
	defer os.Chdir(cwd)
	if err := sh.Run("go", "get", "-u", "github.com/ugorji/go/codec/codecgen"); err != nil {
		return errors.Wrap(err, "could not update codecgen")
	}
	out, err := sh.Output("go", "generate", "-x")
	if err != nil || out == "" {
		return errors.Wrap(err, "failed to go generate new codec")
	}
	return nil
}

func caddyModules() error {
	cmds := []string{
		"go mod init github.com/mholt/caddy",
		"go get github.com/BTBurke/caddy-etcd",
		"go get github.com/bifurcation/mint@v0.0.0-20180715133206-93c51c6ce115",
		"go mod vendor",
		"go mod edit -replace github.com/BTBurke/caddy-etcd=" + cwd,
		"go mod edit -replace go.etcd.io/etcd=../etcd",
		"rm -rf vendor/", // temporary hack because go modules are fucked
		"go build -o " + path.Join(cwd, "caddy") + " ./caddy/main.go",
	}
	if err := os.Chdir(path.Join(tempDir, "caddy")); err != nil {
		return errors.Wrap(err, "could not change to caddy director to build")
	}
	defer os.Chdir(cwd)
	for _, cmd := range cmds {
		c := strings.Split(cmd, " ")
		if err := sh.Run(c[0], c[1:]...); err != nil {
			return errors.Wrapf(err, "failed to run command %s to build caddy", cmd)
		}
	}
	return nil
}

func checkBuild() error {
	out, err := sh.Output("./caddy", "-plugins")
	if err != nil || !strings.Contains(out, "tls.cluster.etcd") {
		return errors.Wrap(err, "build appears to have failed, could not find the tls.cluster.etcd plugin")
	}
	fmt.Println("\n\n\nSUCCESS! The built caddy binary should be in this directory.  You can check that the plugins were inserted successfully by running ./caddy -plugins")
	return nil
}

// global variables to streamline magefile
var tempDir string
var cwd string
var version string
