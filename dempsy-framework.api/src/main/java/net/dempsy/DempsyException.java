/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy;

/**
 * General Dempsy unchecked exception
 */
public class DempsyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public final Throwable userCause;

    public DempsyException(final String message, final Throwable cause, final boolean userCaused) {
        super(message, cause);
        userCause = userCaused ? cause : null;
    }

    public DempsyException(final String message) {
        super(message);
        userCause = null;
    }

    public DempsyException(final Throwable cause, final boolean userCaused) {
        super(cause);
        userCause = userCaused ? cause : null;
    }

    public boolean userCaused() {
        return userCause != null;
    }
}
