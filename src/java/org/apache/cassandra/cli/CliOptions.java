/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cli;

import org.apache.cassandra.thrift.TClientTransportFactory;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.commons.cli.*;
import org.apache.thrift.transport.TFramedTransport;

import java.util.*;

/**
 *
 * Used to process, and act upon the arguments passed to the CLI.
 *
 */
public class CliOptions
{
    private static final CLIOptions options; // Info about command line options

    // Name of the command line tool (used for error messages)
    private static final String TOOL_NAME = "cassandra-cli";

    // Command line options
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final String DEBUG_OPTION = "debug";
    private static final String USERNAME_OPTION = "username";
    private static final String PASSWORD_OPTION = "password";
    private static final String KEYSPACE_OPTION = "keyspace";
    private static final String BATCH_OPTION = "batch";
    private static final String HELP_OPTION = "help";
    private static final String FILE_OPTION = "file";
    private static final String JMX_PORT_OPTION = "jmxport";
    private static final String JMX_USERNAME_OPTION = "jmxusername";
    private static final String JMX_PASSWORD_OPTION = "jmxpassword";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String SCHEMA_MIGRATION_WAIT_TIME = "schema-mwt";

    private static final String SSL_TRUSTSTORE = "truststore";
    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
    private static final String SSL_PROTOCOL = "ssl-protocol";
    private static final String SSL_ALGORITHM = "ssl-alg";
    private static final String SSL_STORE_TYPE = "store-type";
    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    // Default values for optional command line arguments
    private static final String DEFAULT_HOST        = "127.0.0.1";
    private static final int    DEFAULT_THRIFT_PORT = 9160;

    // Register the command line options and their properties (such as
    // whether they take an extra argument, etc.
    static
    {
        options = new CLIOptions();

        options.addOption("h",  HOST_OPTION,     "HOSTNAME", "cassandra server's host name");
        options.addOption("p",  PORT_OPTION,     "PORT",     "cassandra server's thrift port");
        options.addOption("u",  USERNAME_OPTION, "USERNAME", "user name for cassandra authentication");
        options.addOption("pw", PASSWORD_OPTION, "PASSWORD", "password for cassandra authentication");
        options.addOption("k",  KEYSPACE_OPTION, "KEYSPACE", "cassandra keyspace user is authenticated against");
        options.addOption("f",  FILE_OPTION,     "FILENAME", "load statements from the specific file");
        options.addOption(null, JMX_PORT_OPTION, "JMX-PORT", "JMX service port");
        options.addOption(null, JMX_USERNAME_OPTION, "JMX-USERNAME", "JMX service username");
        options.addOption(null, JMX_PASSWORD_OPTION, "JMX-PASSWORD", "JMX service password");
        options.addOption(null, SCHEMA_MIGRATION_WAIT_TIME,  "TIME", "Schema migration wait time (secs.), default is 10 secs");
        options.addOption(
                TClientTransportFactory.SHORT_OPTION,
                TClientTransportFactory.LONG_OPTION,
                "TRANSPORT-FACTORY",
                "Fully-qualified TTransportFactory class name for creating a connection to cassandra");

        // options without argument
        options.addOption("B",  BATCH_OPTION,   "enabled batch mode (suppress output; errors are fatal)");
        options.addOption(null, DEBUG_OPTION,   "display stack-traces (NOTE: We print strack-traces in the places where it makes sense even without --debug)");
        options.addOption("?",  HELP_OPTION,    "usage help");
        options.addOption("v",  VERBOSE_OPTION, "verbose output when using batch mode");
    }

    private static void printUsage()
    {
        new HelpFormatter().printHelp(TOOL_NAME, options);
    }

    public void processArgs(CliSessionState css, String[] args)
    {
        args = setSystemProperties(args);

        CommandLineParser parser = new GnuParser();
        try
        {
            CommandLine cmd = parser.parse(options, args, false);

            if (cmd.hasOption(HOST_OPTION))
            {
                css.hostName = cmd.getOptionValue(HOST_OPTION);
            }
            else
            {
                css.hostName = DEFAULT_HOST;
            }

            String transportFactoryClassName = cmd.hasOption(TClientTransportFactory.LONG_OPTION)
                    ? cmd.getOptionValue(TClientTransportFactory.LONG_OPTION)
                    : System.getProperty(TClientTransportFactory.PROPERTY_KEY, TFramedTransportFactory.class.toString());
            css.transportFactory = validateAndSetClientTransportFactory(transportFactoryClassName);

            if (cmd.hasOption(DEBUG_OPTION))
            {
                css.debug = true;
            }

            // Look for optional args.
            if (cmd.hasOption(PORT_OPTION))
            {
                css.thriftPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
            }
            else
            {
                css.thriftPort = DEFAULT_THRIFT_PORT;
            }

            // Look for authentication credentials (username and password)
            if (cmd.hasOption(USERNAME_OPTION))
            {
                css.username = cmd.getOptionValue(USERNAME_OPTION);
            }
            else
            {
                css.username = "default";
            }

            if (cmd.hasOption(PASSWORD_OPTION))
            {
                css.password = cmd.getOptionValue(PASSWORD_OPTION);
            }
            else
            {
                css.password = "";
            }

            // Look for keyspace
            if (cmd.hasOption(KEYSPACE_OPTION))
            {
                css.keyspace = cmd.getOptionValue(KEYSPACE_OPTION);
            }

            if (cmd.hasOption(BATCH_OPTION))
            {
                css.batch = true;
            }

            if (cmd.hasOption(FILE_OPTION))
            {
                css.filename = cmd.getOptionValue(FILE_OPTION);
            }

            if (cmd.hasOption(JMX_PORT_OPTION))
            {
                css.jmxPort = Integer.parseInt(cmd.getOptionValue(JMX_PORT_OPTION));
            }

            if (cmd.hasOption(JMX_USERNAME_OPTION))
            {
                css.jmxUsername = cmd.getOptionValue(JMX_USERNAME_OPTION);
            }

            if (cmd.hasOption(JMX_PASSWORD_OPTION))
            {
                css.jmxPassword = cmd.getOptionValue(JMX_PASSWORD_OPTION);
            }

            if (cmd.hasOption(HELP_OPTION))
            {
                printUsage();
                System.exit(1);
            }

            if (cmd.hasOption(VERBOSE_OPTION))
            {
                css.verbose = true;
            }

            if (cmd.hasOption(SCHEMA_MIGRATION_WAIT_TIME))
            {
                css.schema_mwt = Integer.parseInt(cmd.getOptionValue(SCHEMA_MIGRATION_WAIT_TIME)) * 1000;
            }

            // Abort if there are any unrecognized arguments left
            if (cmd.getArgs().length > 0)
            {
                System.err.printf("Unknown argument: %s%n", cmd.getArgs()[0]);
                System.err.println();
                printUsage();
                System.exit(1);
            }
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            System.err.println();
            printUsage();
            System.exit(1);
        }
    }

    private static String[] setSystemProperties(String[] args)
    {
        List<String> unprocessedArgs = new ArrayList<String>();
        for (String arg : args)
        {
            if (arg.matches("-D[\\w\\.]+=.*"))
            {
                String[] keyValue = arg.split("=");
                System.setProperty(keyValue[0].substring(2), keyValue[1]);
            }
            else
                unprocessedArgs.add(arg);
        }
        return unprocessedArgs.toArray(new String[unprocessedArgs.size()]);
    }

    private static class CLIOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }


    private static TClientTransportFactory validateAndSetClientTransportFactory(String clientTransportFactory)
    {
        try
        {
            Class factoryClass = Class.forName(clientTransportFactory);

            if (!TClientTransportFactory.class.isAssignableFrom(factoryClass))
                throw new IllegalArgumentException(String.format("transport factory '%s' " +
                                                                 "not derived from TClientTransportFactory", clientTransportFactory));

            TClientTransportFactory factory = (TClientTransportFactory) factoryClass.newInstance();
            configureTransportFactory(factory);
            return factory;
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(String.format("Cannot create a transport factory '%s'.", clientTransportFactory), e);
        }
    }

    private static void configureTransportFactory(TClientTransportFactory factory)
    {
        Map<String, String> options = new HashMap<String, String>();
        for (String optionKey : factory.supportedOptions())
            if (System.getProperty(optionKey) != null)
                options.put(optionKey, System.getProperty(optionKey));
        factory.setOptions(options);
    }
}
