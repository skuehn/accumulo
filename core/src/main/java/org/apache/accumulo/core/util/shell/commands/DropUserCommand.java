/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.shell.commands;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;

public class DropUserCommand extends Command {
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
    String user = cl.getArgs()[0];
    if (shellState.getConnector().whoami().equals(user))
      throw new BadArgumentException("You cannot delete yourself", fullCommand, fullCommand.indexOf(user));
    shellState.getConnector().securityOperations().dropUser(user);
    Shell.log.debug("Deleted user " + user);
    return 0;
  }
  
  @Override
  public String description() {
    return "deletes a user";
  }
  
  @Override
  public String usage() {
    return getName() + " <username>";
  }
  
  @Override
  public void registerCompletion(Token root, Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForUsers(root, completionSet);
  }
  
  @Override
  public int numArgs() {
    return 1;
  }
}
