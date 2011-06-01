#-------------------------------------------------------------------------------
# intermediate.rb
#
# this script holds functions that update various intermediate collections.
#-------------------------------------------------------------------------------

# include files
require 'rubygems'
require 'mongo'

# constants
THREADMILL_DATABASE = 'threadmill'
THREADS_COLLECTION = 'threads'
FORUMS_COLLECTION = 'forum_cache'
BOARDS_COLLECTION = 'board_cache'

# Intermediate module definition
module Intermediate
  
  # function to return a yyyy-mm-dd string corresponding to the UTC timezone from epoch-seconds input
  def self.get_utc_ymd_string_from_epoch_seconds(epoch_seconds_timestamp)
    
    utc_ymd_time_string = Time.at(epoch_seconds_timestamp).getutc.strftime("%Y-%m-%d")
    
  end
  
  # function to update threads collection
  def self.update_threads_collection(docs)

    puts 'entering update_threads_collection'

    # form a connection to Mongo
    db_connection = Mongo::Connection.new(HOST, PORT)

    # connect the target database
    threadmill_data_base = db_connection.db(THREADMILL_DATABASE)

    # get the threads collection
    threads_collection = threadmill_data_base.collection(THREADS_COLLECTION)
    
    # create a hash to hold these per-thread statistics
    threads = Hash.new

    # for each message record, get the required field values and update the stats for the thread
    docs.each do |s|
      
      # get field values
      author = s["author_name"]
      author_id = s["author_id"]
      board = s["board_url"]
      forum = s["forum_id"]
      thread = s["thread_id"] 
      original_post = s["is_op"]
      timestamp = s["timestamp"]
      
      # create an author-timestamp tuple
      author_timestamp_entry = Array.new
      author_timestamp_entry << author_id
      author_timestamp_entry << timestamp
         
      # form a concatenation to use as a hash key
      key_value = board + forum + thread      
      
      # if a record for this thread already exists, update it
      if threads[key_value]
        
        # update total messages
        threads[key_value]["total_messages"] += 1
        
        # update number original messages
        if original_post != 0
          threads[key_value]["original_messages"] += 1
        end       
        
        # update the author-timestamp array
        threads[key_value]["author_timestamp_array"] << author_timestamp_entry
        
      # otherwise, create a record for this thread and add it to the hash  
      else
        
        # create a hash to hold the values for this thread
        new_thread_record = Hash.new
        
        # add these primary fields
        new_thread_record["board_name"] = s["board_name"]
        new_thread_record["board_id"] = s["board_id"]
        new_thread_record["board_url"] = s["board_url"]
        new_thread_record["forum_name"] = s["forum_name"]
        new_thread_record["forum_id"] = s["forum_id"] 
        new_thread_record["thread_title"] = s["thread_title"]      
        new_thread_record["thread_id"] = s["thread_id"]
        new_thread_record["thread_url"] = s["thread_url"] 
        new_thread_record["is_processed"] = 0
        
        # set the value for total messages
        new_thread_record["total_messages"] = 1
        
        # set the value for original messages
        if original_post == 0
          new_thread_record["original_messages"] = 0
        else
          new_thread_record["original_messages"] = 1
        end
        
        # create and add the author-timestamps array
        author_timestamps = Array.new
        author_timestamps << author_timestamp_entry
        new_thread_record["author_timestamp_array"] = author_timestamps
        
        # add this record to the hash of all thread records
        threads[key_value] = new_thread_record
        
      end
    
    end
    
    # now update these stats in Mongo for each record in the hash
    threads.each do |key, thread_record|
      
      # try to get the record from Mongo
      board_id = thread_record["board_id"]
      forum_id = thread_record["forum_id"]      
      thread_id = thread_record["thread_id"]
            
      record_from_mongo = threads_collection.find_one("board_id" => board_id, "forum_id" => forum_id, "thread_id" => thread_id)
      
      # if the record exists, update it
      if record_from_mongo
        
        # calculate new values for total messages, original messages, and the author-timestamp array 
        record_from_mongo["total_messages"] += thread_record["total_messages"]
        record_from_mongo["original_messages"] += thread_record["original_messages"]   
        record_from_mongo["author_timestamp_array"] += thread_record["author_timestamp_array"]
        
        # calculate number authors
        authors = record_from_mongo["author_timestamp_array"].map {|x| x[0]}
        authors.uniq!
        number_authors = authors.size
 
        # calculate first, last, and total days seen in local timezone
        timestamps = record_from_mongo["author_timestamp_array"].map {|x| x[1]}
        
        days_seen = Array.new
        timestamps.each do |x|
          days_seen << get_utc_ymd_string_from_epoch_seconds(x)
        end
        
        days_seen.uniq!
        days_seen.sort!
        
        number_days_seen = days_seen.size
        first_day_seen = days_seen.first
        last_day_seen = days_seen.last           
      
        # now update the changed fields in the db copy of the record and explicitly mark the thread as unprocessed
        threads_collection.update(
          {"_id" => record_from_mongo["_id"]}, 
          {
            "$set" => {
              "total_messages" => record_from_mongo["total_messages"], 
              "original_messages" => record_from_mongo["original_messages"],
              "first_day_active" => first_day_seen,
              "last_day_active" => last_day_seen,
              "total_days_active" => number_days_seen,
              "number_authors" => number_authors,
              "is_processed" => 0
            }, 
            "$pushAll" => {"author_timestamp_array" => thread_record["author_timestamp_array"]}
          }
        )       
        
      # otherwise, add the record
      else
        
        # get the author-timestamp array
        author_timestamps = thread_record["author_timestamp_array"]
        
        # calculate number authors
        authors = author_timestamps.map {|x| x[0]}
        authors.uniq!
        number_authors = authors.size
        
        # calculate first, last, and total days seen in local timezone
        timestamps = author_timestamps.map {|x| x[1]}
        
        days_seen = Array.new
        timestamps.each do |x|
          days_seen << get_utc_ymd_string_from_epoch_seconds(x)
        end
        
        days_seen.uniq!
        days_seen.sort!
        
        number_days_seen = days_seen.size
        first_day_seen = days_seen.first
        last_day_seen = days_seen.last
        
        # add these values to the record
        thread_record["first_day_active"] = first_day_seen
        thread_record["last_day_active"] = last_day_seen
        thread_record["total_days_active"] = number_days_seen
        thread_record["number_authors"] = number_authors

        # insert the record in the collection
        threads_collection.insert(thread_record)
      end

    end

    # close the db connection
    db_connection.close
    
  end
  
  def self.update_forum_cache_collection
    
    puts 'entering update_forum_cache_collection'

    # form a connection to Mongo
    db_connection = Mongo::Connection.new(HOST, PORT)

    # connect the target database
    threadmill_data_base = db_connection.db(THREADMILL_DATABASE)

    # get the threads collection
    threads_collection = threadmill_data_base.collection(THREADS_COLLECTION)
    
    # get the forums collection
    forums_collection = threadmill_data_base.collection(FORUMS_COLLECTION)
    
    # get any unprocessed or updated thread records; convert Mongo cursor to an array
    # so that we can later iterate over the same documents when marking them processed
    unprocessed_threads = threads_collection.find("is_processed" => 0).to_a
    
    # create a hash to hold per-forum statistics
    forums = Hash.new
    
    # process the threads and populate the forum-statistics hash
    unprocessed_threads.each do |thread_doc|
      
      # get field values
      board = thread_doc["board_url"]
      forum = thread_doc["forum_id"]

      # form a concatenation to use as a forum hash key
      key_value = board + forum   
      
      # create a thread key for the per-thread hashes by using the Mongo id of the thread document as a string
      thread_key = thread_doc["_id"].to_s
      
      # if a record for this forum already exists, update it with information for this thread
      if forums[key_value]
      
        forums[key_value]["per_thread_total_messages"][thread_key] = thread_doc["total_messages"]
        forums[key_value]["per_thread_original_messages"][thread_key] = thread_doc["original_messages"]        
                
        author_timestamps = thread_doc["author_timestamp_array"]
      
        authors = author_timestamps.map {|x| x[0]}
        authors.uniq!
        forums[key_value]["authors"] = forums[key_value]["authors"] | authors
        
        timestamps = author_timestamps.map {|x| x[1]}
        days_seen = Array.new
        timestamps.each do |x|
          days_seen << get_utc_ymd_string_from_epoch_seconds(x)
        end
        days_seen.uniq!
        forums[key_value]["days_seen"] = forums[key_value]["days_seen"] | days_seen           
      
      # otherwise, create a record for this forum and add it to the hash  
      else
        
        # create a hash to hold the values for this forum
        new_forum_record = Hash.new
        
        # add these primary fields
        new_forum_record["board_name"] = thread_doc["board_name"]
        new_forum_record["board_id"] = thread_doc["board_id"]
        new_forum_record["board_url"] = thread_doc["board_url"]
        new_forum_record["forum_name"] = thread_doc["forum_name"]
        new_forum_record["forum_id"] = thread_doc["forum_id"] 
      
        # create hashes to hold per-thread information for this forum
        new_forum_record["per_thread_total_messages"] = Hash.new
        new_forum_record["per_thread_total_messages"][thread_key] = thread_doc["total_messages"]
        
        new_forum_record["per_thread_original_messages"] = Hash.new
        new_forum_record["per_thread_original_messages"][thread_key] = thread_doc["original_messages"]        
                 
        # create and populate arrays to hold the authors and days seen for this forum
        new_forum_record["authors"] = Array.new
        new_forum_record["days_seen"] = Array.new
        
        author_timestamps = thread_doc["author_timestamp_array"]
      
        authors = author_timestamps.map {|x| x[0]}
        authors.uniq!
        new_forum_record["authors"] = authors
        
        timestamps = author_timestamps.map {|x| x[1]}
        days_seen = Array.new
        timestamps.each do |x|
          days_seen << get_utc_ymd_string_from_epoch_seconds(x)
        end
        days_seen.uniq!
        new_forum_record["days_seen"] = days_seen
       
        # add an is_processed field for this forum
        new_forum_record["is_processed"] = 0
        
        # add this record to the hash of all forum records
        forums[key_value] = new_forum_record
        
      end
      
    end
        
    # now update these stats in Mongo for each record in the hash
    forums.each do |key, forum_record|
      
      # try to get the record from Mongo
      board_id = forum_record["board_id"]
      forum_id = forum_record["forum_id"]      
            
      record_from_mongo = forums_collection.find_one("board_id" => board_id, "forum_id" => forum_id)
      
      # if the record exists, update it
      if record_from_mongo
        
        forum_record["per_thread_total_messages"].each do |thread_key, number_messages|
          record_from_mongo["per_thread_total_messages"][thread_key] = number_messages
        end
        
        forum_record["per_thread_original_messages"].each do |thread_key, number_messages|
          record_from_mongo["per_thread_original_messages"][thread_key] = number_messages
        end
        
        record_from_mongo["authors"] = record_from_mongo["authors"] | forum_record["authors"]
        
        record_from_mongo["days_seen"] = record_from_mongo["days_seen"] | forum_record["days_seen"]
        
        # update tallies for messages, days, authors, threads
        total_messages = 0
        record_from_mongo["per_thread_total_messages"].each do |key, value|
          total_messages += value
        end
        
        total_original_messages = 0
        record_from_mongo["per_thread_original_messages"].each do |key, value|
          total_original_messages += value
        end    
        
        record_from_mongo["days_seen"].sort!
        
        
        total_days_seen = record_from_mongo["days_seen"].size
        first_day_seen = record_from_mongo["days_seen"].first
        last_day_seen = record_from_mongo["days_seen"].last
        number_authors = record_from_mongo["authors"].size
        number_threads = record_from_mongo["per_thread_total_messages"].size
        
        
        # now update the changed fields in the db copy of the record and explicitly mark the forum as unprocessed
        forums_collection.update(
          {"_id" => record_from_mongo["_id"]}, 
          {"$set" => {
              "per_thread_total_messages" => record_from_mongo["per_thread_total_messages"], 
              "per_thread_original_messages" => record_from_mongo["per_thread_original_messages"],
              "authors" => record_from_mongo["authors"],
              "days_seen" => record_from_mongo["days_seen"],
              "total_messages" => total_messages,
              "original_messages" => total_original_messages,                            
              "total_days_active" => total_days_seen,
              "first_day_active" => first_day_seen,              
              "last_day_active" => last_day_seen,
              "number_authors" => number_authors,                            
              "number_threads" => number_threads,
              "is_processed" => 0}})
        
      # otherwise, add the record 
      else
        
        # update tallies messages, days, authors, threads
        total_messages = 0
        forum_record["per_thread_total_messages"].each do |key, value|
          total_messages += value
        end
        
        total_original_messages = 0
        forum_record["per_thread_original_messages"].each do |key, value|
          total_original_messages += value
        end  
        
        forum_record["days_seen"].sort!
        
        forum_record["total_messages"] = total_messages
        forum_record["original_messages"] = total_original_messages  
        forum_record["total_days_active"] = forum_record["days_seen"].size
        forum_record["first_day_active"] = forum_record["days_seen"].first
        forum_record["last_day_active"] = forum_record["days_seen"].last
        forum_record["number_authors"] = forum_record["authors"].size
        forum_record["number_threads"] = forum_record["per_thread_total_messages"].size
        
        # insert the record in the collection
        forums_collection.insert(forum_record)
        
      end
      
    end
    
    # now, mark these threads as processed
    unprocessed_threads.each do |thread_doc|
      threads_collection.update({"_id" => thread_doc["_id"]}, {"$set" => {"is_processed" => 1}}) 
    end
    
    # close the db connection
    db_connection.close    
    
  end
  
  def self.update_board_cache_collection
    
    puts 'entering update_board_cache_collection'

    # form a connection to Mongo
    db_connection = Mongo::Connection.new(HOST, PORT)

    # connect the target database
    threadmill_data_base = db_connection.db(THREADMILL_DATABASE)
    
    # get the forums collection
    forums_collection = threadmill_data_base.collection(FORUMS_COLLECTION)
    
    # get the boards collection
    boards_collection = threadmill_data_base.collection(BOARDS_COLLECTION)
    
    # get any unprocessed or updated forum records; convert Mongo cursor to an array
    # so that we can later iterate over the same documents when marking them processed
    unprocessed_forums = forums_collection.find("is_processed" => 0).to_a
    
    # create a hash to hold per-board statistics
    boards = Hash.new
    
    # process the forums and populate the board-statistics hash
    unprocessed_forums.each do |forum_doc|

      # create a hash key for the board
      key_value = forum_doc["board_url"]  
      
      # create a forum key for the per-forum hashes by using the Mongo id of the forum document as a string
      forum_key = forum_doc["_id"].to_s
    
      # if a record for this board already exists, update it with information for this forum
      if boards[key_value]
    
        boards[key_value]["per_forum_total_messages"][forum_key] = forum_doc["total_messages"]
        boards[key_value]["per_forum_original_messages"][forum_key] = forum_doc["original_messages"]        
        boards[key_value]["per_forum_thread_counts"][forum_key] = forum_doc["number_threads"]  
                        
        boards[key_value]["authors"] = boards[key_value]["authors"] | forum_doc["authors"]
        boards[key_value]["days_seen"] = boards[key_value]["days_seen"] | forum_doc["days_seen"]
      
      # otherwise, create a record for this board and add it to the hash  
      else
   
        # create a hash to hold the values for this board
        new_board_record = Hash.new
        
        # add these primary fields
        new_board_record["board_name"] = forum_doc["board_name"]
        new_board_record["board_id"] = forum_doc["board_id"]
        new_board_record["board_url"] = forum_doc["board_url"]
    
        # create hashes to hold per-forum information for this board
        new_board_record["per_forum_total_messages"] = Hash.new
        new_board_record["per_forum_total_messages"][forum_key] = forum_doc["total_messages"]
        
        new_board_record["per_forum_original_messages"] = Hash.new
        new_board_record["per_forum_original_messages"][forum_key] = forum_doc["original_messages"]  
        
        new_board_record["per_forum_thread_counts"] = Hash.new
        new_board_record["per_forum_thread_counts"][forum_key] = forum_doc["number_threads"]
              
        # create and populate arrays to hold the authors and days seen for this board
        new_board_record["authors"] = forum_doc["authors"]
        new_board_record["days_seen"] = forum_doc["days_seen"]
        
        # add this record to the hash of all board records
        boards[key_value] = new_board_record
        
      end
      
    end
           
    # now update these stats in Mongo for each record in the hash
    boards.each do |key, board_record|
      
      # try to get the record from Mongo
      board_id = board_record["board_id"]   
            
      record_from_mongo = boards_collection.find_one("board_id" => board_id)
      
      # if the record exists, update it
      if record_from_mongo
      
        board_record["per_forum_total_messages"].each do |forum_key, number_messages|
          record_from_mongo["per_forum_total_messages"][forum_key] = number_messages
        end
        
        board_record["per_forum_original_messages"].each do |forum_key, number_messages|
          record_from_mongo["per_forum_original_messages"][forum_key] = number_messages
        end
        
        board_record["per_forum_thread_counts"].each do |forum_key, number_threads|
          record_from_mongo["per_forum_thread_counts"][forum_key] = number_threads
        end
        
        record_from_mongo["authors"] = record_from_mongo["authors"] | board_record["authors"]
        
        record_from_mongo["days_seen"] = record_from_mongo["days_seen"] | board_record["days_seen"]
        
        # update tallies for messages, days, authors, threads
        total_messages = 0
        record_from_mongo["per_forum_total_messages"].each do |key, value|
          total_messages += value
        end
        
        total_original_messages = 0
        record_from_mongo["per_forum_original_messages"].each do |key, value|
          total_original_messages += value
        end    
        
        total_threads = 0
        record_from_mongo["per_forum_thread_counts"].each do |key, value|
          total_threads += value
        end
        
        record_from_mongo["days_seen"].sort!
        
        total_days_seen = record_from_mongo["days_seen"].size
        first_day_seen = record_from_mongo["days_seen"].first
        last_day_seen = record_from_mongo["days_seen"].last
        number_authors = record_from_mongo["authors"].size
        number_forums = record_from_mongo["per_forum_total_messages"].size    
        
        # now update the changed fields in the db copy of the record and explicitly mark the forum as unprocessed
        boards_collection.update(
          {"_id" => record_from_mongo["_id"]}, 
          {"$set" => {
              "per_forum_total_messages" => record_from_mongo["per_forum_total_messages"], 
              "per_forum_original_messages" => record_from_mongo["per_forum_original_messages"],
              "authors" => record_from_mongo["authors"],
              "days_seen" => record_from_mongo["days_seen"],
              "total_messages" => total_messages,
              "original_messages" => total_original_messages,                            
              "total_days_active" => total_days_seen,
              "first_day_active" => first_day_seen,              
              "last_day_active" => last_day_seen,
              "number_authors" => number_authors,                            
              "number_forums" => number_forums,
              "number_threads" => total_threads}})

      # otherwise, add the record 
      else
     
        # update tallies messages, days, authors, threads
        total_messages = 0
        board_record["per_forum_total_messages"].each do |key, value|
          total_messages += value
        end
        
        total_original_messages = 0
        board_record["per_forum_original_messages"].each do |key, value|
          total_original_messages += value
        end  
        
        total_threads = 0
        board_record["per_forum_thread_counts"].each do |key, value|
          total_threads += value
        end
        
        board_record["days_seen"].sort!
        
        board_record["total_messages"] = total_messages
        board_record["original_messages"] = total_original_messages  
        board_record["total_days_active"] = board_record["days_seen"].size
        board_record["first_day_active"] = board_record["days_seen"].first
        board_record["last_day_active"] = board_record["days_seen"].last
        board_record["number_authors"] = board_record["authors"].size
        board_record["number_forums"] = board_record["per_forum_total_messages"].size
        board_record["number_threads"] = total_threads
                
        # insert the record in the collection
        boards_collection.insert(board_record)
        
      end
      
    end


    
    # now, mark these forums as processed
    unprocessed_forums.each do |forum_doc|
      forums_collection.update({"_id" => forum_doc["_id"]}, {"$set" => {"is_processed" => 1}}) 
    end
    
    # close the db connection
    db_connection.close    
    
  end

end






