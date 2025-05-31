require 'net/http'
require 'json'
require 'csv'
require 'uri'
require 'date'
require 'time'
require 'securerandom'
require 'dotenv'

# Load environment variables
Dotenv.load

class SenseToObs
  def initialize()
    # Validate OBS configuration
    @obs_host =  ENV['OBS_HOST']
    @obs_api_key = ENV['OBS_API_KEY']
    
    if @obs_host.nil? || @obs_api_key.nil?
      puts "Error: OBS configuration is incomplete:"
      puts "  OBS_HOST: #{@obs_host.nil? ? 'missing' : 'configured'}"
      puts "  OBS_API_KEY: #{@obs_api_key.nil? ? 'missing' : 'configured'}"
      puts "Please configure both OBS_HOST and OBS_API_KEY to enable uploads to OpenBikeSensor Portal."
      exit 1
    end

    @last_update_dir = 'last_updates'
    @base_url = 'https://api.opensensemap.org'
    @debug_box_id = ENV['DEBUG_BOX_ID']
    @time_gap_threshold = 3600 # 1 hour in seconds

    
    # Create last_update directory if it doesn't exist
    Dir.mkdir(@last_update_dir) unless Dir.exist?(@last_update_dir)
  end

  def format_duration(seconds)
    return "#{seconds.round(2)} seconds" if seconds < 60
    
    minutes = seconds / 60
    return "#{minutes.round(2)} minutes" if minutes < 60
    
    hours = minutes / 60
    return "#{hours.round(2)} hours" if hours < 24
    
    days = hours / 24
    return "#{days.round(2)} days"
  end

  def format_time(time)
    # Convert to CET/CEST
    cet_time = time.getlocal('+02:00')
    # Format with timezone indicator
    cet_time.strftime('%Y-%m-%d %H:%M:%S %Z')
  end

  def fetch_boxes
    uri = URI("#{@base_url}/boxes?phenomenon=Overtaking+Distance&format=json&grouptag=wiesbaden,bike&minimal=true")
    puts "Fetching boxes from URL: #{uri}"
    response = Net::HTTP.get_response(uri)
    
    if response.is_a?(Net::HTTPSuccess)
      begin
        data = JSON.parse(response.body)
        puts "Successfully fetched #{data.length} boxes"
        data
      rescue JSON::ParserError => e
        puts "Error parsing JSON response: #{e.message}"
        puts "Response body: #{response.body[0..200]}..." # Print first 200 chars of response
        []
      end
    else
      puts "Error fetching boxes: #{response.code} - #{response.message}"
      puts "Response body: #{response.body[0..200]}..." # Print first 200 chars of response
      []
    end
  end

  def get_last_update(box_id)
    last_update_file = File.join(@last_update_dir, "last_update_#{box_id}.txt")
    if File.exist?(last_update_file) && !File.empty?(last_update_file)
      last_update = File.read(last_update_file).strip
      if last_update.empty?
        # If file is empty, return timestamp from 1 year ago
        (Time.now - 365 * 24 * 60 * 60).strftime('%Y-%m-%dT%H:%M:%S.%LZ')
      else
        # Add 5 seconds to the last update time
        (Time.parse(last_update) + 5).strftime('%Y-%m-%dT%H:%M:%S.%LZ')
      end
    else
      # If file doesn't exist, return timestamp from 1 year ago
      (Time.now - 365 * 24 * 60 * 60).strftime('%Y-%m-%dT%H:%M:%S.%LZ')
    end
  end

  def save_last_update(box_id, timestamp)
    last_update_file = File.join(@last_update_dir, "last_update_#{box_id}.txt")
    File.write(last_update_file, timestamp)
  end

  def fetch_box_data(box_ids, phenomenon)
    return [] if box_ids.empty?
    
    # Calculate date range for each box
    box_data = []
    box_ids.each do |box_id|
      start_date = Time.parse(get_last_update(box_id))
      end_date = Time.now
      
      # Set end date to 2 AM of the current day to avoid splitting trips
      end_date = Time.new(end_date.year, end_date.month, end_date.day, 2, 0, 0)
      
      # Format dates in RFC3339 format
      from_date = start_date.strftime('%Y-%m-%dT00:00:00Z')
      to_date = end_date.strftime('%Y-%m-%dT02:00:00Z')
      
      uri = URI("#{@base_url}/boxes/data?boxId=#{box_id}&phenomenon=#{phenomenon}&columns=createdAt,value,lat,lon,height,boxId,boxName,exposure,sensorId,phenomenon,unit,sensorType&format=json&from-date=#{from_date}&to-date=#{to_date}")
      puts "Fetching #{phenomenon} data for box #{box_id} from URL: #{uri}"
      
      begin
        response = Net::HTTP.get_response(uri)
        
        if response.is_a?(Net::HTTPSuccess)
          begin
            data = JSON.parse(response.body)
            puts "Successfully fetched #{phenomenon} data for box #{box_id} from #{from_date} to #{to_date}"
            box_data.concat(data)
          rescue JSON::ParserError => e
            puts "Error parsing JSON response for box #{box_id}: #{e.message}"
            puts "Response body: #{response.body[0..200]}..."
          end
        else
          puts "Error fetching #{phenomenon} data for box #{box_id}: #{response.code} - #{response.message}"
          puts "Response body: #{response.body[0..200]}..."
        end
      rescue StandardError => e
        puts "Error making HTTP request for box #{box_id}: #{e.message}"
      end
    end
    
    box_data
  end

  def group_data_by_box(box_data)
    # Group data by box ID
    grouped_data = {}
    box_data.each do |data|
      box_id = data['boxId']
      grouped_data[box_id] ||= []
      grouped_data[box_id] << data
    end
    grouped_data
  end

  def identify_trips(box_data)
    # Sort data by timestamp
    sorted_data = box_data.sort_by { |data| Time.parse(data['createdAt']) }
    trips = []
    current_trip = []

    sorted_data.each_with_index do |data, index|
      if index == 0
        current_trip << data
        next
      end

      current_time = Time.parse(data['createdAt'])
      previous_time = Time.parse(sorted_data[index - 1]['createdAt'])
      time_gap = current_time - previous_time

      if time_gap > @time_gap_threshold
        # Time gap is greater than threshold, validate and add trip if it meets criteria
        if valid_trip?(current_trip)
          trips << current_trip
        end
        current_trip = [data]
      else
        # Continue current trip
        current_trip << data
      end
    end

    # Add the last trip if it meets criteria
    if valid_trip?(current_trip)
      trips << current_trip
    end
    trips
  end

  def valid_trip?(trip)
    return false if trip.length < 2  # Trip muss mindestens 2 Punkte haben
    
    # Prüfe auf mindestens einen bestätigten Überholvorgang
    trip.any? do |measurement|
      measurement['value'].to_f > 0  # Distanz > 0 cm
    end
  end

  def find_closest_speed_measurement(speed_data, target_time, max_time_diff = 5)
    return nil if speed_data.empty?
    
    # Binary search for the closest timestamp
    left = 0
    right = speed_data.length - 1
    
    while left <= right
      mid = (left + right) / 2
      mid_time = speed_data[mid][:time]
      
      if mid_time == target_time
        return speed_data[mid][:data]
      elsif mid_time < target_time
        left = mid + 1
      else
        right = mid - 1
      end
    end
    
    # Check the closest points around the binary search result
    candidates = []
    [left - 1, left, left + 1].each do |idx|
      next if idx < 0 || idx >= speed_data.length
      time_diff = (speed_data[idx][:time] - target_time).abs
      candidates << [time_diff, speed_data[idx][:data]] if time_diff <= max_time_diff
    end
    
    return nil if candidates.empty?
    candidates.min_by { |time_diff, _| time_diff }[1]
  end

  def convert_speed_to_kmh(speed_ms)
    return nil if speed_ms.nil?
    (speed_ms.to_f * 3.6).round(1)  # Convert m/s to km/h and round to 1 decimal
  end

  def upload_to_obs(trip_data, box_id, trip_index)
    return unless @obs_api_key

    uri = URI("#{@obs_host}/api/tracks")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = uri.scheme == 'https'
    http.read_timeout = 30  # 30 seconds timeout
    
    # Prepare the request
    request = Net::HTTP::Post.new(uri.path)
    request['Authorization'] = "OBSUserId #{@obs_api_key}"
    request['Content-Type'] = 'application/json'
    request['User-Agent'] = 'OBS/SenseBoxToOBS'
    
    # Convert trip data to CSV string
    csv_data = CSV.generate(col_sep: ';') do |csv|
      # Write metadata line
      metadata = {
        'OBSDataFormat' => '2',
        'OBSFirmwareVersion' => 'SenseBoxToOBS',
        'DeviceId' => box_id,
        'DataPerMeasurement' => '1',
        'MaximumMeasurementsPerLine' => '1',
        'OffsetLeft' => '0',
        'OffsetRight' => '0',
        'NumberOfDefinedPrivacyAreas' => '0',
        'TrackId' => SecureRandom.uuid,
        'PrivacyLevelApplied' => 'AbsolutePrivacy',
        'MaximumValidFlightTimeMicroseconds' => '18560',
        'BluetoothEnabled' => '0',
        'PresetId' => 'default',
        'DistanceSensorsUsed' => 'Sensebox-Overtaking-Distance'
      }
      csv << [metadata.map { |k, v| "#{k}=#{v}" }.join('&')]
      
      # Write header
      csv << ['Date', 'Time', 'Millis', 'Comment', 'Latitude', 'Longitude', 'Altitude', 
              'Course', 'Speed', 'HDOP', 'Satellites', 'BatteryLevel', 'Left', 'Right',
              'Confirmed', 'Marked', 'Invalid', 'InsidePrivacyArea', 'Factor', 'Measurements',
              'Tms1', 'Lus1', 'Rus1']
      
      # Add all trip data rows
      trip_data.each do |row|
        csv << row
      end
    end

    # Create multipart form data
    boundary = "----WebKitFormBoundary#{SecureRandom.hex(16)}"
    request['Content-Type'] = "multipart/form-data; boundary=#{boundary}"
    
    # Build multipart body
    body = []
    body << "--#{boundary}"
    body << 'Content-Disposition: form-data; name="title"'
    body << ''
    body << "AutoUpload trip_#{box_id}_#{trip_index + 1}"
    body << "--#{boundary}"
    body << 'Content-Disposition: form-data; name="description"'
    body << ''
    body << "Uploaded with OpenBikeSensor SenseBoxToOBS"
    body << "--#{boundary}"
    body << "Content-Disposition: form-data; name=\"body\"; filename=\"senseBox_trip_#{box_id}_#{trip_index + 1}.csv\""
    body << 'Content-Type: text/csv'
    body << ''
    body << csv_data
    body << "--#{boundary}--"
    
    request.body = body.join("\r\n")
    
    begin
      response = http.request(request)
      if response.is_a?(Net::HTTPSuccess)
        puts "    Successfully uploaded trip to OpenBikeSensor portal"
        # Save the latest timestamp for this box after successful upload
        latest_timestamp = trip_data.last[0..1].join(' ') # Combine date and time from last row
        save_last_update(box_id, latest_timestamp)
        puts "    Updated last update timestamp for box #{box_id} to: #{format_time(Time.parse(latest_timestamp))}"
        return true
      else
        puts "    Failed to upload trip: #{response.code} - #{response.message}"
        puts "    Response body: #{response.body[0..200]}..."
        return false
      end
    rescue StandardError => e
      puts "    Error uploading trip: #{e.message}"
      return false
    end
  end

  def export_trip_to_csv(trip, box_id, trip_index, speed_data)
    # Prepare trip data
    trip_data = []
    
    # Process each measurement
    trip.each do |measurement|
      time = Time.parse(measurement['createdAt'])
      
      # Find closest speed measurement using binary search
      speed_measurement = find_closest_speed_measurement(speed_data, time)
      speed_ms = speed_measurement ? speed_measurement['value'].to_f : nil
      speed_kmh = convert_speed_to_kmh(speed_ms)
      
      # Convert value (left distance) from meters to centimeters
      left_distance = (measurement['value'].to_f * 100).round
      
      # Determine if measurement should be confirmed based on speed and distance
      confirmed = if speed_kmh && speed_kmh < 5
        0  # Not confirmed if speed is below 5 km/h
      else
        measurement['value'].to_f > 0 ? 1 : 0  # Confirmed if distance > 0 and speed is sufficient
      end
      
      # Create row with available data
      row = [
        time.strftime('%d.%m.%Y'),  # Date
        time.strftime('%H:%M:%S'),  # Time
        (time.to_f * 1000).to_i,    # Millis (converted from timestamp)
        '',                         # Comment
        measurement['lat'],         # Latitude
        measurement['lon'],         # Longitude
        measurement['height'],      # Altitude
        '',                         # Course
        speed_kmh || '',            # Speed in km/h
        '',                         # HDOP
        '',                         # Satellites
        '',                         # BatteryLevel
        left_distance,              # Left distance in cm
        '',                         # Right distance (not available)
        confirmed,                  # Confirmed based on speed and distance
        '',                         # Marked
        0,                          # Invalid
        0,                          # InsidePrivacyArea
        58,                         # Factor (default from documentation)
        1,                          # Measurements (1 per line)
        0,                          # Tms1 (no millisecond offset available)
        (left_distance * 58).to_i,  # Lus1 (converted to microseconds using factor)
        ''                          # Rus1 (not available)
      ]
      
      trip_data << row
    end
    
    # Upload to OpenBikeSensor portal if API key is configured
    if @obs_api_key
      upload_to_obs(trip_data, box_id, trip_index)
    else
      puts "    Skipping upload - no API key configured"
    end
  end

  def process_data
    puts "Starting data processing..."
    
    if @debug_box_id
      puts "Processing debug box ID: #{@debug_box_id}"
      box_ids = [@debug_box_id]
    else
      boxes = fetch_boxes
      if boxes.empty?
        puts "No boxes found to process"
        return
      end
      box_ids = boxes.map { |box| box['_id'] }
      puts "Found #{box_ids.length} box IDs to process"
    end
    
    # Fetch distance and speed data separately
    distance_data = fetch_box_data(box_ids, 'Overtaking+Distance')
    speed_data = fetch_box_data(box_ids, 'Speed')
    
    if distance_data.empty?
      puts "No distance data received from boxes"
      return
    end

    # Pre-parse timestamps and sort speed data
    speed_data_by_box = {}
    speed_data.each do |data|
      box_id = data['boxId']
      speed_data_by_box[box_id] ||= []
      speed_data_by_box[box_id] << {
        time: Time.parse(data['createdAt']),
        data: data
      }
    end
    
    # Sort speed data by time for each box
    speed_data_by_box.each do |box_id, data|
      data.sort_by! { |entry| entry[:time] }
    end

    # Group data by box and process trips
    grouped_data = group_data_by_box(distance_data)
    
    puts "\nProcessing data for #{grouped_data.length} boxes:"
    puts "----------------------------------------"
    
    grouped_data.each do |box_id, data|
      puts "\nBox ID: #{box_id}"
      puts "Box Name: #{data.first['boxName']}"
      puts "Total data points: #{data.length}"
      
      # Get pre-sorted speed data for this box
      box_speed_data = speed_data_by_box[box_id] || []
      puts "Found #{box_speed_data.length} speed measurements for this box"
      
      trips = identify_trips(data)
      puts "Identified #{trips.length} trips"
      
      trips.each_with_index do |trip, trip_index|
        start_time = Time.parse(trip.first['createdAt'])
        end_time = Time.parse(trip.last['createdAt'])
        duration = end_time - start_time
        
        # Calculate intervals between consecutive measurements
        intervals = []
        trip.each_cons(2) do |current, next_point|
          current_time = Time.parse(current['createdAt'])
          next_time = Time.parse(next_point['createdAt'])
          intervals << (next_time - current_time)
        end
        
        max_interval = intervals.max || 0
        avg_interval = intervals.empty? ? 0 : intervals.sum / intervals.length

        # Calculate speeds for this trip
        speeds = []
        trip.each do |measurement|
          speed_measurement = find_closest_speed_measurement(box_speed_data, Time.parse(measurement['createdAt']))
          if speed_measurement
            speed_ms = speed_measurement['value'].to_f
            speed_kmh = convert_speed_to_kmh(speed_ms)
            speeds << speed_kmh if speed_kmh && speed_kmh > 0
          end
        end
        
        max_speed = speeds.max || 0
        avg_speed = speeds.empty? ? 0 : speeds.sum / speeds.length
        
        puts "\n  Trip #{trip_index + 1}:"
        puts "    Start: #{format_time(start_time)}"
        puts "    End: #{format_time(end_time)}"
        puts "    Duration: #{format_duration(duration)}"
        puts "    Data points: #{trip.length}"
        puts "    Average interval: #{format_duration(avg_interval)}"
        puts "    Maximum interval: #{format_duration(max_interval)}"
        puts "    Max Speed: #{max_speed} km/h"
        puts "    Avg Speed: #{avg_speed} km/h"
        
        # Export trip to CSV
        export_trip_to_csv(trip, box_id, trip_index, box_speed_data)
      end
      
      puts "----------------------------------------"
    end
  end
end

# Run the script
begin
  sense_to_obs = SenseToObs.new()
  sense_to_obs.process_data
rescue StandardError => e
  puts "Fatal error: #{e.message}"
  puts e.backtrace
end 