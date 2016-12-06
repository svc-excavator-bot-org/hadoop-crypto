/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.hadoop.cipher;

import com.palantir.hadoop.io.SeekableCipherInput;
import com.palantir.io.SeekableInput;
import com.palantir.io.SeekableInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;

/**
 * Decrypts data read from the given {@link FSDataInputStream} using the given {@link SeekableCipher}.
 */
public final class FsCipherInputStream extends FSInputStream {

    private final SeekableInputStream input;

    public FsCipherInputStream(final FSDataInputStream delegate, SeekableCipher cipher) {
        input = new SeekableInputStream(new SeekableCipherInput(new FsSeekableInput(delegate), cipher));
    }

    @Override
    public void seek(long pos) throws IOException {
        input.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return input.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        return input.read();
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        return input.read(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    private static final class FsSeekableInput implements SeekableInput {

        private FSDataInputStream input;

        private FsSeekableInput(FSDataInputStream input) {
            this.input = input;
        }

        @Override
        public void seek(long offset) throws IOException {
            input.seek(offset);
        }

        @Override
        public long getPos() throws IOException {
            return input.getPos();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return input.read(bytes, offset, length);
        }

        @Override
        public int read() throws IOException {
            return input.read();
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

    }

}
