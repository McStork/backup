# encoding: utf-8

## Possible evolutions
# * Have two timeouts: one for calls to flush, segments merge, snapshot, and a different one for the other API's calls
# * Use the ignore_unavailable parameter in other API's calls than just snapshot
# * Have more snapshot options: include_global_state, partial,...
# * Register snapshot repository?

# Note that this module creates an Elasticsearch client (using the official library) 
# that is accessible with reads

require 'elasticsearch'

module Backup
  module Database
    class Elasticsearch < Base
      class Error < Backup::Error; end

      ##
      # List of Elasticsearch nodes
      # Defaults to ['localhost:9200']
      attr_accessor :hosts

      ##
      # Snapshot's repository name
      # It corresponds to the repository registered in the Elasticsearch cluster
      # Defaults to 'mybackup'
      attr_accessor :repository

      ##
      # Name of the indice that needs to get snapshoted.
      # To snapshot all indices, set this to `:all` or leave blank.
      attr_accessor :indice

      ##
      # Connection timeout in seconds
      # Calls to the flush, segments merge or snapshot API can take some time
      # to execute.
      # Defaults to 600
      attr_accessor :timeout

      ##
      # Set to true to specify that missing indices should not cause
      # the snapshot to fail.
      # Defaults to false
      attr_accessor :ignore_unavailable

      ##
      # Snapshot name
      # Defaults to 'snapshot%Y.%m.%d.%Hh%Mm%ss'
      attr_accessor :snapshot

      ##
      # Specify the indices time rotation
      # Possible values are :daily, :weekly, :monthly
      #
      # When set, #indice acts as a prefix
      attr_accessor :time_based
      
      ##
      # Specify the date separator
      #
      # Defaults to '.'
      # Is only effective for time-based indices
      attr_accessor :date_splitter

      ##
      # Specify which indice to backup based on its time rotation
      #
      # For example, if #ago has a value of 1 and #time_based is set with :daily,
      # then backup yesterday's indice
      #
      # Defaults to 1
      # Is only effective for time-based indices
      attr_accessor :ago

      ##
      # Set to true to check the time rotated indice completeness before backup
      # The check is done by verifying that indice's #ago - 1 exists
      # This way the indice will only be backuped if events' integration is over for the indice
      #
      # Defaults to false
      # Is only effective for time-based indices
      attr_accessor :strict

      ##
      # Backuped #time_based indice's name, built from #time_based and #ago
      #
      # Is only effective for time-based indices
      attr_reader :time_indice

      ##
      # #time_based (indice + 1) name, built from #time_based and #ago
      #
      # Is only effective for time-based indices
      attr_reader :time_indice_plus_one
      
      ##
      # Credentials for the Elasticsearch clusters' HTTP Basic auth
      attr_accessor :username, :password

      ##
      # Specify the connection scheme
      # Defaults to 'http'
      attr_accessor :scheme

      ##
      # Enable the validation of the server's certificate
      # Defaults to true when scheme is 'https'.
      attr_accessor :validate_ssl

      ##
      # Self-signed .cer or .pem file to validate the server's certificate
      attr_accessor :cacert
    
      ##
      # Set to true to disable write operations against the indice. It updates
      # the index's settings. 
      #
      # Defaults to false.
      # This operation is run before the creation of the snapshot.
      # Useful for time-based indices who should not index more data.
      attr_accessor :blocks_write
      
      ##
      # Sets the maximum number of Lucene segments that should be kept on each shard.
      #
      # This operation is run before the creation of the snapshot.
      # Segments merging can take some time, depending on the indice size.
      # Useful for time-based indices that should not index more data.
      attr_accessor :max_num_segments

      ##
      # Set to true to commit a Lucene segment if the Translog is not empty.
      #
      # Defaults to false.
      # This operation is run before the creation of the snapshot.
      attr_accessor :flush

      ##
      # Elasticsearch client
      attr_reader :es_client

      ##
      # Init Elasticsearch client
      def initialize(model, database_id = nil, &block)
        super
        instance_eval(&block) if block_given?

        check_configuration

        @repository ||= 'mybackup'
        @indice     ||= :all
        @snapshot   ||= 'snapshot' << DateTime.now.strftime('%Y.%m.%d.%Hh%Mm%Ss')
        @timeout    ||= 60 * 10
        @scheme     ||= 'http'
        @hosts      ||= ['localhost:9200']

        if @scheme == 'https'
          @validate_ssl = true if @validate_ssl.nil?
        end

        if @time_based
          init_time_indices
        end

        if ['http','https'].include? @scheme
          init_client
        end
      end

      def init_time_indices
        @ago           ||= 1
        @date_splitter ||= '.'
        
        now = DateTime.now

        @time_indice = calc_time_indice(now, ago)

        if @strict
          @time_indice_plus_one = calc_time_indice(now, (ago.to_i - 1))
        end
      end
      private :init_time_indices
      
      def calc_time_indice(date, n)
        format = ""
        case time_based
          when :daily then
            date = date.prev_day(n)
            format = "#{ @indice }%Y#{ @date_splitter }%m#{ @date_splitter }%d"
          when :weekly then
            date = date.prev_day(n*7)
            format = "#{ @indice }%G#{ @date_splitter }%V"
          when :monthly then
            date = date.prev_month(n)
            format = "#{ @indice }%Y#{ @date_splitter }%m"
        end
        date.strftime(format)
      end
      private :calc_time_indice

      def init_client
        params = {}
        
        params[:hosts] = @hosts
        params[:user] = @username if @username
        params[:password] = @password if @password
        params[:http] = { :scheme => @scheme }
        params[:reload_on_failure] = true

        ssl = {}
        ssl[:verify] = !!@validate_ssl
        ssl[:ca_file] = @cacert if (!!@validate_ssl && @cacert)
        request = {}
        request[:timeout] = @timeout
        params[:transport_options] = { :ssl => ssl, :request => request }

        @es_client = ::Elasticsearch::Client.new params
      end
      private :init_client

      def check_configuration
        unsigned = %w{ max_num_segments timeout }
        if unsigned.map {|name| send(name) }.any? { |i| i.is_a?(Integer) && i < 1 }
          raise Error, <<-EOS
            Configuration Error
            #{ unsigned.map {|name| "##{ name }"}.join(', ') } must be >= 1
          EOS
        end
      end

      def indice_exists(indice)
        @es_client.indices.exists index: indice
      end

      ##
      # 1. Verify that the snapshot repository exists
      # 2. OPT: Verify that the indice +1 exists
      # 3. Verify that the indice exists
      # 4. OPT: Flush indice
      # 5. OPT: Block writes on the indice
      # 6. OPT: Merge the indice to N segments per shard
      # 7. Create snapshot, dumped in the folder configured in Elasticsearch's #repository
      #
      def perform!
        super

        # snapshot.verify_repository (elasticsearch gem) already throws an exception if answer has code > 300
        @es_client.snapshot.verify_repository repository: @repository

        if not @time_based
          if snapshot_all?
            indice = '_all'
          else
            indice = @indice
          end

          if not indice_exists indice
            raise Error, "indice '#{ indice }' does not exist"
          end

          snapshot_indice indice
        else
          if @time_indice_plus_one
            if not indice_exists @time_indice_plus_one
              raise Error, "indice plus one '#{ @time_indice_plus_one }' does not exist"
            end
          end
          if not indice_exists @time_indice
            raise Error, "indice '#{ @time_indice }' does not exist"
          end

          snapshot_indice @time_indice
        end
      end

      def expect_acknowledge(failure_message)
        response_body = yield
        if (response_body != { 'acknowledged' => true })
          raise Error, failure_message
        end
      end
      
      def expect_no_failure(failure_message)
        response_body = yield
        shards = '_shards'
        if (not response_body.key?(shards)) || (response_body[shards]['failed'] != 0)
          raise Error, failure_message
        end
      end

      def do_flush(indice)
        if @flush
          expect_no_failure "failed to flush indice '#{ indice }'" do
            @es_client.indices.flush_synced index: indice
          end
        end
      end

      def do_blocks_write(indice)
        if @blocks_write
          expect_acknowledge "failed to update settings of indice '#{ indice }'" do
            @es_client.indices.put_settings index: indice, body: { index: { blocks: { write: true } } }
          end
        end
      end

      def do_merge_segments(indice)
        if @max_num_segments
          expect_no_failure "failed to merge segments of indice '#{ indice }'" do
            @es_client.indices.forcemerge index: indice, max_num_segments: @max_num_segments
          end
        end
      end

      def do_snapshot(indice)
        body = {}
        body[:ignore_unavailable] = !!@ignore_unavailable
        if not snapshot_all?
          body[:indices] = indice
        end

        response_body = @es_client.snapshot.create repository: @repository, snapshot: @snapshot, wait_for_completion: true, body: body
        snap = 'snapshot'
        state = 'state'
        if (not response_body.key?(snap)) || (response_body[snap][state] != 'SUCCESS')
          raise Error, "failed to create snapshot of indice '#{ indice }'"
        end
      end

      def snapshot_indice(indice)
        do_flush indice

        do_blocks_write indice
        
        do_merge_segments indice

        do_snapshot indice
      end

      def snapshot_all?
        @indice == :all
      end

    end
  end
end
