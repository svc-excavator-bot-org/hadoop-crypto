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

package com.palantir.io;

import java.io.DataInputStream;
import java.io.IOException;

public final class SeekableDataInput extends DataInputStream implements SeekableInput {

    private final SeekableInput input;

    public SeekableDataInput(SeekableInput in) {
        super(new SeekableInputStream(in));
        input = in;
    }

    @Override
    public void seek(long offset) throws IOException {
        input.seek(offset);
    }

    @Override
    public long getPos() throws IOException {
        return input.getPos();
    }

}
