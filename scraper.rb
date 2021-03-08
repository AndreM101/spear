require 'faraday'
require 'JSON'
require 'scraperwiki'
require 'Date'

$token
begin
	$db_rows = ScraperWiki.sqliteexecute("SELECT council_reference FROM swdata").to_a
rescue
	$db_rows = []
end
"""
Request and extract the description from the link seen below, modularised due to a separate rest call
Returns the retrieved description, or an empty string if there is none
"""
def extract_description(url)
  begin
	  resp = Faraday.get(url) do |req|
		req.headers['authorization'] = "Bearer #{$token}"
	  end
	  if JSON.parse(resp.body)['data'].nil?
		$token = get_token
		puts "New token generated"
		resp = Faraday.get(url) do |req|
			req.headers['authorization'] = "Bearer #{$token}"
		end
		linkId = JSON.parse(resp.body)['data']['header']['applicationId'].to_s
		puts resp.body.to_s + "\n"
		puts linkId
	  else
			linkId = JSON.parse(resp.body)['data']['applicationId'].to_s
	  end
		
	  #puts "LID: " + resp.body
	  link = "https://www.spear.land.vic.gov.au/spear/api/v1/applications/#{linkId}/summary"
	  #puts link
	  resp = Faraday.get(link) do |req|
		req.headers['authorization'] = "Bearer #{$token}"
	  end
	  #puts JSON.parse(resp.body.to_s + '\n\n')
	  if JSON.parse(resp.body)['data'].nil?
		return ""
	  end
	  JSON.parse(resp.body)['data']['intendedUse']
  rescue
    return ''
  end
end



"""
Request an authorization token from spear 
Returns the given token as a string
"""
def get_token
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/oauth/token'

  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = 'Basic Y2xpZW50YXBwOg=='
    req.headers['content-type'] = 'application/x-www-form-urlencoded'
    req.body = 'username=public&password=&grant_type=password&client_id=clientapp&scope=spear_rest_api'
  end

  JSON.parse(resp.body)['access_token']
end


"""
Requests and returns a set of the next 35 rows (the API call only returns 35 at a time) for a given ID and starting row number
Returns the raw row data as an array of hashes
"""
def get_set_of_rows(id, start_row)
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/applicationlist/publicSearch'
  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{$token}"
    req.headers['content-type'] = 'application/json'
    req.headers['accept'] = '*/*'

    req.body = '{ "data": { "applicationListSearchRequest": { "searchFilters": [{ "id": "completed", "selected": ["ALL"] }], "searchText": null, "myApplications": false, "watchedApplications": false, "searchInitiatedByUserClickEvent": false, "sortField": "SPEAR_REF", "sortDirection": "desc", "startRow": ' + start_row.to_s + '}, "tab": "ALL", "filterString": "", "completedFilterString": "ALL", "responsibleAuthoritySiteId": ' + id.to_s + '  } }'
  end

  JSON.parse(resp.body)['data']['resultRows']
end


"""
Finds the number of rows for a given ID
Returns the count as an integer
"""
def get_row_count(id)
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/applicationlist/publicSearch'
  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{$token}"
    req.headers['content-type'] = 'application/json'
    req.headers['accept'] = '*/*'

    req.body = '{ "data": { "applicationListSearchRequest": { "searchFilters": [{ "id": "completed", "selected": ["ALL"] }], "searchText": null, "myApplications": false, "watchedApplications": false, "searchInitiatedByUserClickEvent": false, "sortField": "SPEAR_REF", "sortDirection": "desc", "startRow": 0 }, "tab": "ALL", "filterString": "", "completedFilterString": "ALL", "responsibleAuthoritySiteId": '+ id.to_s + '  } }' 
  end
  row_count = JSON.parse(resp.body)['data']['numFound']
  puts "Found #{row_count} rows for id #{id}"
  row_count
end

"""
Retrieves the entire set of rows for a given id and cutoff date (only rows after this date will be passed along, a nil value will fetch all dates)
Returns an array of hashes
"""
def get_whole_id_set(id, cutoff_date)
  # Rows can only be gathered in groups of 35, and indexed by the starting row number, so these must be calculated to avoid unnecessary calls
  row_count = get_row_count(id)
  number_of_iterations = (row_count / 35).floor + 1
  seenRows = []
  complete_row_set = []
  (0..number_of_iterations).each do |start|
    output = get_set_of_rows(id, start * 35)
    output.each do |row|
	  next if seenRows.include?(row['spearReference'])
	  next if not check_date(row['submittedDate'], cutoff_date)
	  
      complete_row_set.append(row)
      seenRows.append(row['spearReference'])
    end
  end

  complete_row_set
end


"""
Parse raw row data into planningalerts compliant records
Returns an array of hashes
"""
def process_row_set(row_set)
  set_of_records = []
  # puts row_set.to_s
  counter = 1
  total = row_set.length
  row_set.each do |row|
    #begin
  	  counter += 1
	  puts "Processing row " + counter.to_s + " out of " + total.to_s
	  if not check_db(row['spearReference'])
  	    
	    info_url = "https://www.spear.land.vic.gov.au/spear/api/v1/applications/retrieve/#{row['spearReference']}?publicView=true"
        record = {
          # We're using the SPEAR Ref # because we want that to be unique across the "authority"
          'council_reference' => row['spearReference'],
          'address' => row['property'],
          'info_url' => info_url,
          # 'comment_url' => comment_url,
          'date_scraped' => Date.today.to_s
        }
        record['date_received'] = Date.strptime(row['submittedDate'], '%d/%m/%Y').to_s if row['submittedDate']

        # Get more detailed information by going to the application detail page (but only if necessary)
        begin
          record['description'] = extract_description(info_url).to_s
	    rescue
	      $token = new_token()
		  record['description'] = extract_description(info_url).to_s
	    end
        record['description'] = row['applicationTypeDisplay'] if record['description'] == ''
        set_of_records.append(record)
	end
  end
  set_of_records
end


"""
Find all available council IDs
Returns a list of integers representing valid IDs
"""
def find_ids
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/site/search'

  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{$token}"
    req.headers['content-type'] = 'application/json'

    req.body = '{"data":{"searchType":"publicsearch","searchTypeFilter":"all","searchText":null,"showInactiveSites":false}}'
  end

  id_list = []

  JSON.parse(resp.body)['data'].each do |item|
    id_list.append(item['id'])
  end
  id_list
end


"""
Main function for the script, accepts a cutoff date as input
	- rows after the cutoff date will be included
	- a nil value will include all dates
Returns the final dataset as an array of rows
"""
def scrape(cutoff_date=Date.today.prev_day)
  cutoff_date = cutoff_date.strftime('%d/%m/%Y')
  $token = get_token
  id_set = find_ids
  all_rows = []
  puts "Found #{id_set.length} council datasets"
  id_set.each do |id|
    output = get_whole_id_set(id, cutoff_date)
    puts "Gathered #{output.length} rows for id #{id}"
    all_rows += output
  end
  puts "#{all_rows.length} rows collected"
  data = process_row_set(all_rows)
  # puts data.to_s
  puts "Done, #{data.length} new rows found"
  #return data
  total = data.length
  data.each do |record|
	ScraperWiki.save_sqlite(['council_reference'], record)
  end
end


# Checks database for a given row
def check_db(id)
  row = {"council_reference"=>id}
  return $db_rows.include? row
end

# determines whether a row is within the cutoff date or not
def check_date(row_date, cutoff_date)
    if cutoff_date.nil?
		return true
	elsif row_date.nil?
		return false
	end
	row_split = row_date.split("/")
	cutoff_split = cutoff_date.split("/")
	if cutoff_split[2].to_i < row_split[2].to_i
		return true
	elsif cutoff_split[2].to_i == row_split[2].to_i && cutoff_split[1].to_i < row_split[1].to_i
		return true
	elsif cutoff_split[2].to_i == row_split[2].to_i && cutoff_split[1].to_i == row_split[1].to_i && cutoff_split[2].to_i <= row_split[2].to_i
		return true
	else
		return false
	end
end

scrape


