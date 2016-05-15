# encoding: utf-8

require File.expand_path('../../spec_helper.rb', __FILE__)

module Backup
  describe Database::Elasticsearch do
    let(:model) { Model.new(:test_trigger, 'test label') }
    let(:db) { Database::Elasticsearch.new(model) }

    it_behaves_like 'a class that includes Config::Helpers'
    it_behaves_like 'a subclass of Database::Base'

    describe '#initialize' do
      before (:all) { Timecop.freeze(Time.local(2016, 4, 20, 16, 20, 0)) }

      after (:all) { Timecop.return }

      it 'provides default values' do
        expect( db.hosts                ).to eq ['localhost:9200']
        expect( db.repository           ).to eq 'mybackup'
        expect( db.indice               ).to eq :all
        expect( db.snapshot             ).to eq 'snapshot2016.04.20.16h20m00s'
        expect( db.timeout              ).to eq 600
        expect( db.ignore_unavailable   ).to be_nil
        expect( db.time_based           ).to be_nil
        expect( db.ago                  ).to be_nil
        expect( db.date_splitter        ).to be_nil
        expect( db.strict               ).to be_nil
        expect( db.time_indice          ).to be_nil
        expect( db.time_indice_plus_one ).to be_nil
        expect( db.max_num_segments     ).to be_nil
        expect( db.blocks_write         ).to be_nil
        expect( db.flush                ).to be_nil
        expect( db.username             ).to be_nil
        expect( db.password             ).to be_nil
        expect( db.scheme               ).to eq 'http'
        expect( db.validate_ssl         ).to be_nil
        expect( db.cacert               ).to be_nil
        expect( db.es_client            ).to be
      end

      it 'provides default values for #scheme = https' do
        db = Database::Elasticsearch.new(model, :my_id) do |es|
          es.scheme = 'https'
        end
        expect( db.scheme        ).to eq 'https'
        expect( db.validate_ssl  ).to eq true
        expect( db.es_client     ).to be
      end
      
      it 'disables ssl check when #scheme = https' do
        db = Database::Elasticsearch.new(model, :my_id) do |es|
          es.scheme = 'https'
          es.validate_ssl = false
        end
        expect( db.scheme        ).to eq 'https'
        expect( db.validate_ssl  ).to eq false
        expect( db.es_client     ).to be
      end
      
      describe '#initialize time-based indices' do
        it 'provides default daily indice name' do
          name_oracle = 'all2016.04.19'

          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.time_based = :daily
          end
          expect( db.time_indice          ).to eq name_oracle
          expect( db.time_indice_plus_one ).to be_nil
        end
        it 'provides default weekly indice name' do
          name_oracle = 'all2016.15' # 2016, week 15: from 04-11 to 04-17

          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.time_based = :weekly
          end
          expect( db.time_indice          ).to eq name_oracle
          expect( db.time_indice_plus_one ).to be_nil
        end
        it 'provides default monthly indice name' do
          name_oracle = 'all2016.03'

          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.time_based = :monthly
          end
          expect( db.time_indice          ).to eq name_oracle
          expect( db.time_indice_plus_one ).to be_nil
        end

        it "provides default indice + 1 name" do
          indice_name_oracle = 'all2016.04.19'
          indice_name_plus_one_oracle = 'all2016.04.20'

          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.time_based = :daily
            es.strict = true
          end
          expect( db.time_indice          ).to eq indice_name_oracle
          expect( db.time_indice_plus_one ).to eq indice_name_plus_one_oracle
        end

        it "provides default indices name from times ago" do
          indice_name_oracle = 'all2016.04.17'
          indice_name_plus_one_oracle = 'all2016.04.18'

          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.time_based = :daily
            es.ago = 3
            es.strict = true
          end
          expect( db.time_indice          ).to eq indice_name_oracle
          expect( db.time_indice_plus_one ).to eq indice_name_plus_one_oracle
        end

      end
      
      it 'provides every value' do
        db = Database::Elasticsearch.new(model, :my_id) do |es|
          es.hosts = ['myhost:9200']
          es.repository = 'my_repository'
          es.indice = 'my_indice-'
          es.snapshot = 'my_snapshot'
          es.timeout = 900
          es.ignore_unavailable = true
          es.time_based = :monthly
          es.ago = 2
          es.date_splitter = '-'
          es.strict = true
          es.max_num_segments = 1
          es.blocks_write = true
          es.flush = true
          es.username = 'my_username'
          es.password = 'my_password'
          es.scheme = 'https'
          es.validate_ssl = true
          es.cacert = 'my_file_path'
        end

        expect( db.hosts                 ).to eq ['myhost:9200']
        expect( db.repository            ).to eq 'my_repository'
        expect( db.indice                ).to eq 'my_indice-'
        expect( db.snapshot              ).to eq 'my_snapshot'
        expect( db.timeout               ).to eq 900
        expect( db.ignore_unavailable    ).to eq true
        expect( db.time_based            ).to eq :monthly
        expect( db.ago                   ).to eq 2
        expect( db.date_splitter         ).to eq '-'
        expect( db.strict                ).to eq true
        expect( db.time_indice           ).to eq 'my_indice-2016-02'
        expect( db.time_indice_plus_one  ).to eq 'my_indice-2016-03'
        expect( db.max_num_segments      ).to eq 1
        expect( db.blocks_write          ).to eq true
        expect( db.flush                 ).to eq true
        expect( db.username              ).to eq 'my_username'
        expect( db.password              ).to eq 'my_password'
        expect( db.scheme                ).to eq 'https'
        expect( db.validate_ssl          ).to eq true
        expect( db.cacert                ).to eq 'my_file_path'
        expect( db.es_client             ).to be
      end
      
      it 'raises error for unvalid #max_num_segment' do
        expect do
          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.repository = 'mybackup'
            es.max_num_segments = 0
          end
        end.to raise_error(Error, /.*#max_num_segments.* must be >= 1/)
      end
      
      it 'raises error for unvalid #timeout' do
        expect do
          db = Database::Elasticsearch.new(model, :my_id) do |es|
            es.repository = 'mybackup'
            es.timeout = 0
          end
        end.to raise_error(Error, /.*#timeout.* must be >= 1/)
      end

    end # describe '#initialize'
    
    describe '#perform!' do
      before (:all) { Timecop.freeze(Time.local(2016, 4, 20, 16, 20, 0)) }
      after (:all) { Timecop.return }
      
      it 'performs basic auth over https' do
        stub_request(:any, /.*myhost:9200.*/).
            to_return({status: 200}, {status: 404})

        db = Database::Elasticsearch.new(model, :my_id) do |es|
          es.hosts = ['myhost:9200']
          es.username = 'myusername'
          es.password = 'mypassword'
          es.scheme = 'https'
        end

        expect do
          db.perform!
        end.to raise_error(Error)
        expect(a_request(:any, /https:\/\/myhost:9200.*/).
          with(basic_auth: ['myusername', 'mypassword'])).to have_been_made.at_least_once
      end

      it 'raises an error in strict mode' do
        stub_request(:any, /.*localhost:9200.*/).
            to_return({status: 200}, {status: 404})

        db = Database::Elasticsearch.new(model, :my_id) do |es|
          es.indice = 'my_indice-'
          es.time_based = :daily
          es.ago = 1
          es.strict = true
        end

        expect do
          db.perform!
        end.to raise_error(Error, /indice plus one 'my_indice-2016.04.20' does not exist/)
      end

      context "successfull shards updates" do
        before (:each) do
           stub_request(:any, /.*localhost:9200.*/).
             to_return({status: 200, headers: {'Content-Type' => 'application/json'},
                        body: "{\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0}}"})
        end
        let(:db) { Database::Elasticsearch.new(model, :my_id) }
        
        it 'flushes successfully' do
          expect do
            db.do_flush db.indice
          end.not_to raise_error
        end
        
        it 'merges segments successfully' do
          expect do
            db.do_merge_segments db.indice
          end.not_to raise_error
        end
      end # context "successfull shards updates"

      context "successfull acknowledged updates" do
        before (:each) do
          stub_request(:any, /.*localhost:9200.*/).
              to_return({status: 200, headers: {'Content-Type' => 'application/json'}, body: "{\"acknowledged\":true}"})
        end
        let(:db) { Database::Elasticsearch.new(model, :my_id) }
        
        it 'blocks write successfully' do
          expect do
            db.do_blocks_write db.indice
          end.not_to raise_error
        end

      end # context "successfull acknowledged updates"

      context "perform well configured calls to the snapshot's API" do
        before (:each) do
          stub_request(:any, /.*/).
            to_return(status: 200, headers: {'Content-Type' => 'application/json'},
                       body: "{\"snapshot\":{\"snapshot\":\"snapshot_1\",\"state\":\"SUCCESS\",\"failures\":[],"+
                       "\"shards\":{\"total\":10,\"failed\":0,\"successful\":10}}}")
        end

        it 'creates a snapshot successfully' do
          expect do
            db.do_snapshot db.indice
          end.not_to raise_error
        end

        it "sets 'ignore_unavailable'" do
          db = Database::Elasticsearch.new(model, :my_id) do |db|
            db.ignore_unavailable = true
          end
          db.do_snapshot db.indice
          expect(a_request(:any, /.*/).
                      with(body: /.*"ignore_unavailable":true.*/)).to have_been_made.once
        end
        
        it "does not set 'ignore_unavailable" do
          db = Database::Elasticsearch.new(model, :my_id) do |db|
            db.ignore_unavailable = false
          end
          db.do_snapshot db.indice
          expect(a_request(:any, /.*/).
                      with(body: /.*"ignore_unavailable":false.*/)).to have_been_made.once
        end

        it "sets a custom indice" do
          db = Database::Elasticsearch.new(model, :my_id) do |db|
            db.indice = 'my_indice'
          end
          db.do_snapshot db.indice
          expect(a_request(:any, /.*/).
                      with(body: /.*"indices":"my_indice".*/)).to have_been_made.once
        end

        it "does not set an :all indice" do
          db = Database::Elasticsearch.new(model, :my_id) do |db|
            db.indice = :all
          end
          db.do_snapshot db.indice
          expect(a_request(:any, /.*/).
                           with(body: /.*"indices":".*all"/)).not_to have_been_made
        end

      end # context "perform the call to the snapshot's API"

    end # describe '#perform!'
  end
end
