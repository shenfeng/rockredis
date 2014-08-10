package main


// list encoding:

// key => prefix(l):count(4 byte):min-seq(4-byte):max-seq(4-byte):add-ts(4-byte):update-ts(4-byte):[size(varint):content] (up to ListMaxZiplistEntries)

// \0key$seq => prefix(1):content
