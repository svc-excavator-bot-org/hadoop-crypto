/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.crypto2.hadoop.benchmarks;

import com.google.common.io.ByteStreams;
import com.palantir.crypto2.cipher.AesCtrCipher;
import com.palantir.crypto2.cipher.SeekableCipher;
import com.palantir.crypto2.cipher.SeekableCipherFactory;
import com.palantir.crypto2.io.DecryptingSeekableInput;
import com.palantir.crypto2.io.DefaultSeekableInputStream;
import com.palantir.crypto2.keys.KeyMaterial;
import com.palantir.seekio.InMemorySeekableDataInput;
import com.palantir.seekio.SeekableInput;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

public class BenchmarkReadWrite {

    private static final Random random = new Random();
    private static final int MB = 1024 * 1024;

    @State(Scope.Benchmark)
    public static class MyState {
        public SeekableCipher cipher;
        public byte[] rawBytes;
        public SeekableInputChannel encryptedInputChannel;
        public SeekableInput encryptedBytes;
        public Properties properties;
        public KeyMaterial keyMaterial;

        @Setup
        public void setup() throws IOException {
            cipher = SeekableCipherFactory.getCipher(AesCtrCipher.ALGORITHM);
            rawBytes = new byte[100 * MB];
            random.nextBytes(rawBytes);

            Path file = Files.createTempFile("crypto", "enc");
            FileOutputStream os = new FileOutputStream(file.toFile());
            CipherOutputStream eos = new CipherOutputStream(os, cipher.initCipher(Cipher.ENCRYPT_MODE));

            eos.write(rawBytes);
            eos.close();

            encryptedInputChannel = new SeekableInputChannel(FileChannel.open(file));
            encryptedBytes = new InMemorySeekableDataInput(Files.readAllBytes(file));

            properties = new Properties();
            properties.setProperty(CryptoCipherFactory.CLASSES_KEY, CryptoCipherFactory.CipherProvider.OPENSSL.getClassName());

            keyMaterial = AesCtrCipher.generateKeyMaterial();
        }

    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void fileRead(MyState state) throws IOException {
        readFully(state.encryptedInputChannel);
    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void javaFileRead(MyState state) throws IOException {
        readFully(new DecryptingSeekableInput(state.encryptedInputChannel, state.cipher));
    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void apacheFileRead(MyState state) throws IOException {
        readFully(getApacheCryptoStream(state, state.encryptedInputChannel));
    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void memRead(MyState state) throws IOException {
        readFully(state.encryptedBytes);
    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void javaMemRead(MyState state) throws IOException {
        readFully(new DecryptingSeekableInput(state.encryptedBytes, state.cipher));
    }

    @Benchmark
    @Threads(value = 1)
    @Warmup(iterations = 0)
    @BenchmarkMode(value = Mode.SingleShotTime)
    public void apacheMemRead(MyState state) throws IOException {
        readFully(getApacheCryptoStream(state, state.encryptedBytes));
    }

    private static CryptoInputStream getApacheCryptoStream(MyState state, SeekableInput input)
            throws IOException {
        DefaultSeekableInputStream is = new DefaultSeekableInputStream(input);
        SecretKey key = state.keyMaterial.getSecretKey();
        IvParameterSpec iv = new IvParameterSpec(state.keyMaterial.getIv());
        return new CryptoInputStream(AesCtrCipher.ALGORITHM, state.properties, is, key, iv);
    }

    private static void readFully(SeekableInput input) throws IOException {
        ByteStreams.toByteArray(new DefaultSeekableInputStream(input));
    }

    private static void readFully(InputStream input) throws IOException {
        ByteStreams.toByteArray(input);
    }

}
