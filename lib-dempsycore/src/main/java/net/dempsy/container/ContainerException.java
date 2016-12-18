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

package net.dempsy.container;

/**
 *  This is the base exception class for all container-related problems.
 */
public class ContainerException extends Exception
{
   private static final long serialVersionUID = 1L;
   private boolean expected = false;

   public ContainerException(String message, Throwable cause) { super(message, cause);  }

   public ContainerException(String message) { super(message); }
   
   public boolean isExpected() { return expected; }
   
   public void setExpected(boolean expected) { this.expected = expected; }
}
