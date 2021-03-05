require 'Faraday'
require 'JSON'
require 'scraperwiki'

"""
Request and extract the description from the link seen below, modularised due to a separate rest call
Returns the retrieved description, or an empty string if there is none
"""
def extract_description(url, access_token)
  resp = Faraday.get(url) do |req|
    req.headers['authorization'] = "Bearer #{access_token}"
  end
  linkId = JSON.parse(resp.body)['data']['applicationId'].to_s
  link = "https://www.spear.land.vic.gov.au/spear/api/v1/applications/#{linkId}/summary"

  resp = Faraday.get(link) do |req|
    req.headers['authorization'] = "Bearer #{access_token}"
  end
  if JSON.parse(resp.body)['data'].nil?
	return ""
  end
  JSON.parse(resp.body)['data']['intendedUse']
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
Requests and returns a set of 35 rows (the API call only returns 35 at a time) for a given ID and access token
Returns the raw row data as an array of hashes
"""
def get_set_of_rows(id, access_token, start_row)
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/applicationlist/publicSearch'
  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{access_token}"
    req.headers['content-type'] = 'application/json'
    req.headers['accept'] = '*/*'

    req.body = '{ "data": { "applicationListSearchRequest": { "searchFilters": [{ "id": "completed", "selected": ["ALL"] }], "searchText": null, "myApplications": false, "watchedApplications": false, "searchInitiatedByUserClickEvent": false, "sortField": "SPEAR_REF", "sortDirection": "desc", "startRow": ' + start_row.to_s + '}, "tab": "ALL", "filterString": "", "completedFilterString": "ALL", "responsibleAuthoritySiteId": ' + id.to_s + '  } }'
  end

  JSON.parse(resp.body)['data']['resultRows']
end

"""
Finds the number of rows for a given ID and access token
Returns the count as an integer
"""
def get_row_count(id, access_token)
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/applicationlist/publicSearch'
  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{access_token}"
    req.headers['content-type'] = 'application/json'
    req.headers['accept'] = '*/*'

    req.body = '{ "data": { "applicationListSearchRequest": { "searchFilters": [{ "id": "completed", "selected": ["ALL"] }], "searchText": null, "myApplications": false, "watchedApplications": false, "searchInitiatedByUserClickEvent": false, "sortField": "SPEAR_REF", "sortDirection": "desc", "startRow": 0 }, "tab": "ALL", "filterString": "", "completedFilterString": "ALL", "responsibleAuthoritySiteId": '+ id.to_s + '  } }' 
  end
  row_count = JSON.parse(resp.body)['data']['numFound']
  puts "Found #{row_count} rows for id #{id}"
  row_count
end

"""
Retrieves the entire set of rows for a given id and access token
Returns an array of hashes
"""
def get_whole_id_set(id, access_token)
  # Rows can only be gathered in groups of 35, and indexed by the starting row number, so these must be calculated to avoid unnecessary calls
  row_count = get_row_count(id, access_token)
  number_of_iterations = (row_count / 35).floor + 1
  seenRows = []
  complete_row_set = []
  (0..number_of_iterations).each do |start|
    output = get_set_of_rows(id, access_token, start * 35)
    output.each do |row|
	  # if this row happens to have been retrieved already, ignore it to avoid repeat rows
      next if seenRows.include?(row['spearReference'])
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
def process_row_set(row_set, access_token)
  set_of_records = []
  # puts row_set.to_s
  row_set.each do |row|
    begin
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
      record['description'] = extract_description(info_url, access_token).to_s
      record['description'] = row['applicationTypeDisplay'] if record['description'] == ''
      set_of_records.append(record)
	rescue
		puts "Failed to process " + row
	end
  end
  set_of_records
end

"""
Find all available council IDs
Returns a list of integers representing valid IDs
"""
def find_ids(access_token)
  url = 'https://www.spear.land.vic.gov.au/spear/api/v1/site/search'

  resp = Faraday.post(url) do |req|
    req.headers['authorization'] = "Bearer #{access_token}"
    req.headers['content-length'] = '107'
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
Main function for the script
Returns the final dataset as an array of rows
"""
def scrape
  token = get_token
  id_set = find_ids(token)
  all_rows = []
  puts "Found #{id_set.length} council datasets"
  id_set.each do |id|
    output = get_whole_id_set(id, token)
    puts "Gathered #{output.length} rows for id #{id}"
    all_rows += output
  end
  new_token = get_token
  data = process_row_set(all_rows, new_token)
  # puts data.to_s
  puts "Done, #{data.length} rows found"
  return data
  # data.each do |record|
	#ScraperWiki.save_sqlite(['council_reference'], record)
  #end
end

scrape
