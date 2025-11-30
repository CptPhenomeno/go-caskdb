package caskdb

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	Storage *os.File
	Offset  uint32
	Map     map[string]*KeyEntry
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	diskStore := &DiskStore{Storage: file, Offset: 0, Map: make(map[string]*KeyEntry)}

	for {
		header := make([]byte, headerSize)
		readBytes, err := file.Read(header)

		if err == io.EOF || readBytes == 0 {
			break
		}

		if err != nil {
			return nil, err
		}

		timestamp, keySize, valueSize := decodeHeader(header)
		totalSize := headerSize + keySize + valueSize

		keyBytes := make([]byte, keySize)
		_, err = file.Read(keyBytes)
		if err != nil {
			return nil, err
		}

		key := string(keyBytes)
		keyEntry := NewKeyEntry(timestamp, diskStore.Offset, totalSize)
		diskStore.Map[key] = &keyEntry

		diskStore.Offset += totalSize
		_, err = file.Seek(int64(valueSize), io.SeekCurrent)
	}

	return diskStore, nil
}

func (d *DiskStore) Get(key string) string {
	keyEntry := d.Map[key]
	if keyEntry == nil {
		return ""
	}
	_, err := d.Storage.Seek(int64(keyEntry.Position), io.SeekStart)
	if err != nil {
		return ""
	}

	row := make([]byte, keyEntry.TotalSize)
	_, err = d.Storage.Read(row)
	if err != nil {
		return ""
	}

	_, _, value := decodeKV(row)
	return value
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	size, row := encodeKV(timestamp, key, value)
	keyEntry := NewKeyEntry(timestamp, d.Offset, uint32(size))

	_, err := d.Storage.Seek(int64(d.Offset), io.SeekStart)
	if err != nil {
		panic(err.Error())
	}

	_, err = d.Storage.Write(row)
	if err != nil {
		panic(err.Error())
	}

	err = d.Storage.Sync()
	if err != nil {
		panic(err.Error())
	}

	d.Offset += uint32(size)
	d.Map[key] = &keyEntry
}

func (d *DiskStore) Close() bool {
	err := d.Storage.Sync()
	if err != nil {
		return false
	}

	err = d.Storage.Close()
	if err != nil {
		return false
	}

	return true
}
