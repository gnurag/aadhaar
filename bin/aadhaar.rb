#!/usr/bin/env ruby

# Checks
if RUBY_VERSION < '1.9'
  puts "We need ruby-1.9 or greater to run."
  exit
end


# Requires
require 'rubygems'
require 'active_support/time'
require 'yajl/json_gem'
require 'tire'
require 'csv'
require 'digest/sha1'

# Settings
AADHAAR_DATA_DIR = "/path/to/aadhaar/data"
ES_URL          = "http://localhost:9200"
ES_INDEX        = 'aadhaar'
ES_TYPE         = "UID"
BATCH_SIZE      = 1000

# Helpers
def get_date_from_filename(csvfile)
  return Date.parse csvfile.split('-').last.gsub('.csv', '')
end

def bulk_index(es_index, data)
  Tire.index es_index  do
    import data if data.length > 0
  end
end

# Define a mapping as per the UIDAI CSV
# 
# Registrar,Enrolment Agency,State,District,Sub District,Pin
# Code,Gender,Age,Aadhaar generated,Enrolment Rejected,Residents
# providing email,Residents providing mobile number
#
def create_index(es_index, es_type)
  Tire.index es_index do
    if not exists?
      create :mappings => {
        es_type.to_sym => {
          :properties => {
            :date             => { :type => 'date'},
            :registrar        => { :type => 'string', :analyzer => 'keyword' },
            :agency           => { :type => 'string', :analyzer => 'keyword' },
            :state            => { :type => 'string', :analyzer => 'keyword' },
            :district         => { :type => 'string', :analyzer => 'keyword' },
            :subdistrict      => { :type => 'string', :analyzer => 'keyword' },
            :pincode          => { :type => 'string', :analyzer => 'keyword' },
            :gender           => { :type => 'string', :analyzer => 'keyword' },
            :age              => { :type => 'string' },
            :generated        => { :type => 'string' },
            :rejected         => { :type => 'string' },
            :email            => { :type => 'string' },
            :mobile           => { :type => 'string' }
          }
        }
      }
      sleep 5 # Fresh index, give it time to create shards.
    end
  end
end


# Main iterator
create_index(ES_INDEX, ES_TYPE) # if it does not exists.

Dir.entries(AADHAAR_DATA_DIR).grep(/\.csv/) do |csvfile|
  print "* Parsing #{csvfile} ["
  headers = [:registrar, :agency, :state, :district, :subdistrict, :pincode, :gender, :age, :generated, :rejected, :email, :mobile]
  buffer = []
  first_row = true
  
  CSV.foreach("#{AADHAAR_DATA_DIR}/#{csvfile}", :headers => headers) do |row|
    if not first_row
      record_date = get_date_from_filename(csvfile)
      id_string   = "#{record_date.strftime('%Y%m%d')},#{row}".strip

      doc             = row.to_hash
      doc[:id]        = Digest::SHA1.hexdigest id_string
      doc[:type]      = ES_TYPE
      doc[:date]      = record_date
      doc[:age]       = doc[:age].to_i
      doc[:generated] = doc[:generated].to_i
      doc[:rejected]  = doc[:rejected].to_i
      doc[:email]     = doc[:email].to_i
      doc[:mobile]    = doc[:mobile].to_i

      buffer << doc
      if buffer.length > BATCH_SIZE
        bulk_index(ES_INDEX, buffer)
        buffer.clear
        print "."
      end
    end
    first_row = false if first_row
  end
  # Bulk index any left over documents
  bulk_index(ES_INDEX, buffer)
  puts "]"
end
