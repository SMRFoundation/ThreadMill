#!/usr/bin/ruby

#-------------------------------------------------------------------------------
# reset_threadmill.rb
#
# this script marks all the records in the message database as unprocessed and 
# drops all collections in the threadmill database.  it then recreates the required
# indices.
#-------------------------------------------------------------------------------

# include files
require 'rubygems'
require 'mongo'

# constants
HOST = '127.0.0.1'
PORT = 27017
MESSAGE_DATABASE = 'threadmill'
MESSAGE_COLLECTION = 'message_collection'
THREADMILL_DATABASE = 'threadmill'
THREADS_COLLECTION = 'threads'
FORUMS_COLLECTION = 'forum_cache'
BOARDS_COLLECTION = 'board_cache'

# form a connection to Mongo
db_connection = Mongo::Connection.new(HOST, PORT)

# connect the message database
message_data_base = db_connection.db(MESSAGE_DATABASE)

# get the record collection
record_collection = message_data_base.collection(MESSAGE_COLLECTION)

# get the mongo cursor for all the docs in the main collection that have been processed
docs = record_collection.find("is_processed" => 1)

# mark all the processed records as unprocessed
docs.each do |s|
  record_collection.update({"_id" => s["_id"]}, {"$set" => {"is_processed" => 0}}) 
end

# destroy any intermediate results in the threadmill database
threadmill_data_base = db_connection.db(THREADMILL_DATABASE)
thread_collection = threadmill_data_base.collection(THREADS_COLLECTION)
thread_collection.drop
forum_collection = threadmill_data_base.collection(FORUMS_COLLECTION)
forum_collection.drop
board_collection = threadmill_data_base.collection(BOARDS_COLLECTION)
board_collection.drop

# recreate the indices for the threadmill database
thread_collection.create_index("is_processed")
thread_collection.create_index([["board_id", Mongo::ASCENDING], ["forum_id", Mongo::ASCENDING], ["thread_id", Mongo::ASCENDING]])
forum_collection.create_index("board_id")
forum_collection.create_index("forum_id")
board_collection.create_index("board_id")

# close the db connection
db_connection.close
