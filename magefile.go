// +build mage

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

func init() {
	var err error
	tempDir, err = ioutil.TempDir("", "caddy")
	if err != nil {
		log.Fatal(err)
	}
	cwd, err = os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
}

// declare any extra plugins here that you want built into Caddy.  The map should be the import
// name of the plugin and the import path.
var plugins = map[string]string{
	"etcd": "github.com/BTBurke/caddy-etcd",
}

// If you plan on compiling with plugins that are not already listed on Caddyserver.com, use this
// to declare where you want them inserted in the plugin list  Sometimes precendence of these plugins matters.
// Use `after(<plugin>)` to declare where you want your plugin inserted.  For example, if you want to insert
// plugin `foo` after caddy-jwt, you can use `"foo": after("jwt")`.  See the file `caddyserver/httpserver/plugin.go` for
// the precedence list built into Caddy.
var plugAfter = map[string]string{
	"etcd": "git",
}

// A build step that requires additional params, or platform specific steps for example
func Build() error {
	mg.Deps(cloneCaddy, cloneEtcd)
	fmt.Printf("caddy and etcd cloned to %s\n", tempDir)
	mg.Deps(insertPlugins, generateEtcd)
	mg.SerialDeps(caddyModules)

	return nil
}

func cloneCaddy() error {
	dir := path.Join(tempDir, "caddy")
	if err := sh.Run("git", "clone", "--depth=1", "https://github.com/mholt/caddy", dir); err != nil {
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
	if err := os.Chdir(path.Join(tempDir, "etcd")); err != nil {
		return errors.Wrap(err, "could not change to etcd to regenerate")
	}
	defer os.Chdir(cwd)
	if err := sh.Run("go", "get", "-u", "github.com/ugorji/go/codec/codecgen"); err != nil {
		return errors.Wrap(err, "could not update codecgen")
	}
	if err := sh.Run("go", "generate"); err != nil {
		return errors.Wrap(err, "failed to go generate new codec")
	}
	return nil
}

func caddyModules() error {

}

// global variables to streamline magefile
var tempDir string
var cwd string
