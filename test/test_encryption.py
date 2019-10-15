# -*- coding: utf-8 -*-
from pybossa.encryption import AESWithGCM


class TestAes(object):

    def setUp(self):
        iv_length = 12
        tag_length = 16
        secret = 'very secret'
        self.aes = AESWithGCM(secret, iv_length, tag_length)

    def test_aes(self):
        text = b'testing simple encryption'
        encrypted = self.aes.encrypt(text)
        assert encrypted != text
        decrypted = self.aes.decrypt(encrypted)
        assert decrypted == text

    def test_aes_2(self):
        original = b'this is a test string I plan to encrypt'
        encrypted = 'DMj4/yC2pgzgAg76TApmk7zVZlaG0B47KASCnS/TqH6fQpA9UaHjmGLHqCfvGVVQcSivX76Oy349QivZjOJ2yfXZRb0='
        secret = 'this is my super secret key'
        aes = AESWithGCM(secret)
        assert aes.decrypt(encrypted) == original

    def test_aes_unicode(self):
        text = '∀ z ∈ ℂ, ζ(z) = 0 ⇒ ((z ∈ -2ℕ) ∨ (Re(z) = -½))'
        encrypted = self.aes.encrypt(text.encode())
        decrypted = self.aes.decrypt(encrypted).decode()
        assert text == decrypted
