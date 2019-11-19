package main

import (
	"github.com/bitkumakichi/gohdfs"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func checkPath(client *hdfs.Client, path string) (bool, bool) {
	info, err := client.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, false
		}
		fatal(err)
	}
	return true, info.IsDir()
}

func copyFile(client *hdfs.Client, source string, dest string) error {
	sourceReader, err := client.Open(source)
	if err != nil {
		return err
	}
	defer sourceReader.Close()

	destWriter, err := client.Create(dest)
	if err != nil {
		return err
	}
	defer destWriter.Close()

	_, err = io.Copy(destWriter, sourceReader)
	return err
}

func copyDir(client *hdfs.Client, source string, dest string) error {
	err := client.Walk(source, func(path string, info os.FileInfo, err error) error {
		fullDest := filepath.Join(dest, strings.TrimPrefix(path, source))
		if info.IsDir() {
			if err = client.Mkdir(fullDest, 0755); err != nil {
				return err
			}
		} else {
			if err = copyFile(client, path, fullDest); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func copySingleFile(client *hdfs.Client, source string, dest string) {
	isExist, isDir := checkPath(client, dest)
	if isExist {
		if isDir {
			_, name := path.Split(source)
			dest = path.Join(dest, name)
		} else {
			fatal("Target file is exist.")
		}
	}
	if err := copyFile(client, source, dest); err != nil {
		fatal(err)
	}
}

func copySingleDir(client *hdfs.Client, source string, dest string) {
	isExist, isDir := checkPath(client, dest)
	if isExist {
		if isDir {
			_, name := path.Split(source)
			dest = path.Join(dest, name)
		} else {
			fatal("Target is a file.")
		}
	}
	if err := copyDir(client, source, dest); err != nil {
		fatal(err)
	}
}

func cp(paths []string, recursive bool) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}
	if len(paths) != 2 {
		fatalWithUsage("Only a source and a destination are required.")
	}
	if hasGlob(paths[0]) || hasGlob(paths[1]) {
		fatal("Glob is not supported right now.")
	}
	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}
	dest := paths[len(paths)-1]
	sources, err := expandPaths(client, paths[:len(paths)-1])
	if err != nil {
		fatal(err)
	}

	// single file or dir is supported
	if len(sources) == 1 {
		source := sources[0]

		// check source exist
		sourceExist, sourceIsDir := checkPath(client, source)
		if !sourceExist {
			fatal("Source path not exist.")
		}

		// begin copy
		if sourceIsDir {
			if !recursive {
				fatalWithUsage("Source is a dir.")
			}
			copySingleDir(client, source, dest)
		} else {
			copySingleFile(client, source, dest)
		}
	}
}
