In-kernel searches

in-kernel searches on uncompressed tables are generally much faster (10x) than standard queries as well as PostgreSQL (5x).

s name is shuffle, and because it can greatly benefit compression and it does not take many CPU resources (see below for a justification), it is active by default in PyTables whenever compression is activated (independently of the chosen compressor). It is deactivated when compression is off (which is 