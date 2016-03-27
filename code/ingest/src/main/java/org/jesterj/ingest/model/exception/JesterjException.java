/*
 * Copyright 2016 Needham Software LLC
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

package org.jesterj.ingest.model.exception;

/**
 * Represents the base class for all JesterJ exceptions.
 * 
 * @author dgoldenberg
 */
// TODO should this extend Exception ?
public class JesterjException extends RuntimeException {

  private static final long serialVersionUID = -2456418590249607090L;

  public JesterjException(String message) {
    super(message);
  }

  public JesterjException(Throwable cause) {
    super(cause);
  }

  public JesterjException(String message, Throwable cause) {
    super(message, cause);
  }
}
