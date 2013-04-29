Introduction
============

WordCount hadoop example: Inserts a bunch of words across multiple rows,
and counts them, with RandomPartitioner. The word_count_counters example sums
the value of counter columns for a key.


Running
=======

First build and start a Cassandra server with the default configuration*, 
then run

contrib/word_count$ ant
contrib/word_count$ bin/word_count_setup
contrib/word_count$ bin/word_count


Read the code in src/ for more details.

The word_count_counters example sums the counter columns for a row. The output
is written to a text file in /tmp/word_count.

*If you want to point wordcount at a real cluster, modify the seed
and listenaddress settings accordingly.


Troubleshooting
===============

word_count uses conf/log4j.properties to log to wc.out.
