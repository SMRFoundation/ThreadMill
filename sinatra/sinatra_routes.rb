#!/usr/bin/ruby

#-------------------------------------------------------------------------------
# sinatra_routes.rb
#
# this script defines sinatra routes for displaying threadmill pages.
#-------------------------------------------------------------------------------


# include files
require 'rubygems'
require 'mongo'
require 'sinatra'
require 'haml'

# constants
HOST = '127.0.0.1'
PORT = 27017
DATABASE_NAME = 'threadmill'
THREADS_COLLECTION = 'threads'
FORUMS_COLLECTION = 'forum_cache'
BOARDS_COLLECTION = 'board_cache'

# define the default page
get '/' do
  File.read('public/index.html')
end

# sinatra route for obtaining a single board
get '/boards/:board_id' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the board collection
  board_collection = message_data_base.collection(BOARDS_COLLECTION)

  # get the mongo cursor for the doc
  this_id = BSON::ObjectId.from_string(params[:board_id])
  target_doc = board_collection.find_one("_id" => this_id)
  
  @single_board_data = Array.new
  
  @single_board_data << target_doc["board_url"]
  @single_board_data << target_doc["total_days_active"] 
  @single_board_data << target_doc["number_forums"]
  @single_board_data << target_doc["number_threads"]
  @single_board_data << target_doc["number_authors"]
  @single_board_data << target_doc["total_messages"]
  @single_board_data << target_doc["original_messages"]
  @single_board_data << target_doc["first_day_active"]
  @single_board_data << target_doc["last_day_active"]
    
  haml :single_board
  
end

# sinatra route for obtaining a single forum
get '/forums/:forum_id' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the forum collection
  forum_collection = message_data_base.collection(FORUMS_COLLECTION)

  # get the mongo cursor for the doc
  this_id = BSON::ObjectId.from_string(params[:forum_id])
  target_doc = forum_collection.find_one("_id" => this_id)
  
  @single_forum_data = Array.new
  
  @single_forum_data << target_doc["board_url"]
  @single_forum_data << target_doc["forum_name"]
  @single_forum_data << target_doc["total_days_active"]
  @single_forum_data << target_doc["number_threads"]
  @single_forum_data << target_doc["number_authors"]
  @single_forum_data << target_doc["total_messages"]
  @single_forum_data << target_doc["original_messages"]
  @single_forum_data << target_doc["first_day_active"]
  @single_forum_data << target_doc["last_day_active"]
    
  haml :single_forum
  
end

# sinatra route for obtaining a single thread
get '/threads/:thread_id' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the thread collection
  thread_collection = message_data_base.collection(THREADS_COLLECTION)

  # get the mongo cursor for the doc
  this_id = BSON::ObjectId.from_string(params[:thread_id])
  target_doc = thread_collection.find_one("_id" => this_id)
  
  @single_thread_data = Array.new
  
  @single_thread_data << target_doc["board_url"]
  @single_thread_data << target_doc["forum_name"]
  @single_thread_data << target_doc["thread_id"]
  @single_thread_data << target_doc["total_days_active"]
  @single_thread_data << target_doc["number_authors"]
  @single_thread_data << target_doc["total_messages"]
  @single_thread_data << target_doc["original_messages"]
  @single_thread_data << target_doc["first_day_active"]
  @single_thread_data << target_doc["last_day_active"]
  @single_thread_data << target_doc["thread_url"]
      
  haml :single_thread
  
end

# sinatra route for boards page
get '/boards' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the board collection
  board_collection = message_data_base.collection(BOARDS_COLLECTION)

  # get the mongo cursor for all the docs in the message collection
  docs = board_collection.find
  
  @board_data = Array.new
  
  docs.each do |board_doc|

    board_data_item = Array.new
        
    board_data_item << board_doc["board_url"]
    board_data_item << board_doc["first_day_active"]
    board_data_item << board_doc["last_day_active"]
    board_data_item << board_doc["total_days_active"]
    board_data_item << board_doc["number_forums"]
    board_data_item << board_doc["number_threads"]
    board_data_item << board_doc["number_authors"]
    board_data_item << board_doc["_id"].to_s
    
    @board_data << board_data_item
    
  end
  
  # sort by days active
  @board_data = @board_data.sort_by{|x| -x[3]} 
  
  haml :boards
  
end

# sinatra route for forums page
get '/forums' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the forum collection
  forum_collection = message_data_base.collection(FORUMS_COLLECTION)

  # get the mongo cursor for all the docs in the forum collection
  docs = forum_collection.find
  
  @forum_data = Array.new
  
  docs.each do |forum_doc|

    forum_data_item = Array.new
    
    forum_data_item << forum_doc["board_url"]
    forum_data_item << forum_doc["forum_name"]
    forum_data_item << forum_doc["first_day_active"]
    forum_data_item << forum_doc["last_day_active"]
    forum_data_item << forum_doc["total_days_active"]
    forum_data_item << forum_doc["number_threads"]
    forum_data_item << forum_doc["number_authors"]
    forum_data_item << forum_doc["_id"].to_s
    
    @forum_data << forum_data_item
    
  end

  # sort by days active  
  @forum_data = @forum_data.sort_by{|x| -x[4]} 
  
  haml :forums
  
end

# sinatra route for threads page
get '/threads' do
  
  # form a connection to Mongo
  db_connection = Mongo::Connection.new(HOST, PORT)

  # connect the target database
  message_data_base = db_connection.db(DATABASE_NAME)

  # get the thread collection
  thread_collection = message_data_base.collection(THREADS_COLLECTION)

  # get the mongo cursor for all the docs in the thread collection
  docs = thread_collection.find
  
  @thread_data = Array.new
  
  docs.each do |thread_doc|

    thread_data_item = Array.new
    
    thread_data_item << thread_doc["board_url"]
    thread_data_item << thread_doc["forum_name"]
    thread_data_item << thread_doc["thread_id"]
    thread_data_item << thread_doc["first_day_active"]
    thread_data_item << thread_doc["last_day_active"] 
    thread_data_item << thread_doc["total_days_active"]
    thread_data_item << thread_doc["number_authors"]
    thread_data_item << thread_doc["_id"].to_s
    
    @thread_data << thread_data_item
    
  end
  
  # sort by days active
  @thread_data = @thread_data.sort_by{|x| -x[5]} 
  
  haml :threads
  
end



