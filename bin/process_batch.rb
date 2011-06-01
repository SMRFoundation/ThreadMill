#!/usr/bin/ruby

#-------------------------------------------------------------------------------
# process_batch.rb
#
# this script processes a chunk of messages from the messages database.  
# it should be called frequently enough to keep up with any database additions.  
# the chunk_size can be set to control the memory footprint of the application.
#-------------------------------------------------------------------------------

# include files
require 'rubygems'
require 'mongo'
require 'intermediate'

# constants
HOST = '127.0.0.1'
PORT = 27017
MESSAGE_DATABASE = 'threadmill'
MESSAGE_COLLECTION = 'message_collection'
MAX_CHUNK_SIZE = 10000

# form a connection to Mongo
db_connection = Mongo::Connection.new(HOST, PORT)

# connect the target database
message_data_base = db_connection.db(MESSAGE_DATABASE)

# get the record collection
record_collection = message_data_base.collection(MESSAGE_COLLECTION)

# get the mongo cursor for all the docs in the message collection that are not yet processed
docs = record_collection.find("is_processed" => 0)

# add specified number of unprocessed documents to the document array
puts 'starting batch assembly'
doc_array = Array.new
count = 0
docs.each do |s|

    doc_array << s
    count += 1
    break if count == MAX_CHUNK_SIZE

end

# output the size of the batch
puts 'batch of size ' + count.to_s + ' assembled'

# do work on the document array here
puts 'starting batch processing'

Intermediate.update_threads_collection(doc_array)
Intermediate.update_forum_cache_collection
Intermediate.update_board_cache_collection

puts 'finished batch processing'

# now mark these records as processed
puts 'marking records in this batch as processed'

doc_array.each do |s|
  record_collection.update({"_id" => s["_id"]}, {"$set" => {"is_processed" => 1}}) 
end

puts 'completed marking records as processed'

# close the db connection
db_connection.close






































