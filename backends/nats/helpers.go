package nats

import (
	"crypto/rand"
	"errors"
	"golang.org/x/crypto/chacha20poly1305"
	"os"
	"time"
)

// Simple helper function to convert config int to time.Duration
func helperSecondsToDuration(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}

// Simple helper function to check file exists
func exists(path string, checkDirectory bool) (bool, error) {
	info, err := os.Stat(path)
	if !checkDirectory && err == nil && !info.IsDir() {
		return true, nil
	}
	if checkDirectory && err == nil && info.IsDir() {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Simple helper function to check file is readable
func fileExistsAndIsReadable(path string) error {
	exists, err := exists(path, false)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("file does not exist")
	}
	fil, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	fil.Close()
	return nil
}

// Simple encryption helper
func helperEncrypt(key []byte, plaintext []byte) ([]byte, error) {
	var ciphertext []byte
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plaintext)+aead.Overhead())
	_, err = rand.Read(nonce)
	if err != nil {
		return nil, err
	}
	ciphertext = aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Simple decryption helper
func helperDecrypt(key []byte, ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aead.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}
	nonce, cipher := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	return aead.Open(nil, nonce, cipher, nil)
}
