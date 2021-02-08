module ResqueTools
  extend self

  def self.redis
    Resque.redis
  end

  # structure {"class": "SomeClass", "args": ["a", "b"]}
  def clean_by_class(queue, move_to_queue=nil)
    raise "no block" unless block_given?

    key = "queue:#{queue}"
    key_bak = key + '_bak'
    key_bad = "queue:#{move_to_queue || "#{queue}_bad"}"
    redis.rename(key, key_bak)
    len = redis.llen(key_bak)
    index = moved = 0
    while (job = redis.lpop(key_bak))
      if yield(Oj.load(job))
        redis.rpush(key_bad, job)
        moved += 1
      else
        redis.rpush(key, job)
      end
      index += 1
      print("\r#{index}/#{len}. Moved: #{moved}")
    end
    puts " Done!"
  end

  def count_by_class(queues = :all)
    queues = redis.smembers('queues') if queues == :all
    queues.each do |queue|
      puts "--- #{queue} ---"
      key = "queue:#{queue}"
      jobs = redis.lrange(key, 0, -1)
      puts(jobs.group_by{ |j| Oj.load(j)['class'] }.map{ |c,ar| [c, ar.size] }.sort_by{ |c, n| -n }.map{ |c, n| "#{c}: #{n}"})
      jobs.each { |j| yield(Oj.load(j)) } if block_given?
      puts "Total: #{jobs.size}"
    end
  end

  def size_by_class(queues = :all)
    queues = redis.smembers('queues') if queues == :all
    queues.each do |queue|
      puts "--- #{queue} ---"
      key = "queue:#{queue}"
      jobs = redis.lrange(key, 0, -1)
      puts(jobs.group_by{ |j| Oj.load(j)['class'] }.map{ |c,ar| [c, ar.max{|j| j.size }] }.sort_by{ |c, n| -n }.map{ |c, n| "#{c}: #{n}"})
      puts "Total: #{jobs.size}"
    end
  end


  # Example row in 'failed' queue
  #  {"failed_at"=>"2020/04/27 15:37:26 UTC",
  #   "payload"=>
  #    {"class"=>"Background::Dynect",
  #     "args"=>["activate_user", "chet@cfl.rr.com"]},
  #   "exception"=>"Faraday::TimeoutError",
  #   "error"=>"Net::ReadTimeout with #<TCPSocket:(closed)>",
  #   "backtrace"=>
  #    ["/usr/local/rvm/rubies/ruby-2.6.2/lib/ruby/2.6.0/net/protocol.rb:217:in `rbuf_fill'",
  #     "/home/deploy/src/git/sapi/lib/background/log_with_log_stasher.rb:46:in `perform'"],
  #   "worker"=>
  #    "i-023ffa6ee4933dbc2.resque-worker-misc:21232:unbounce,slow_background,background,immediate_emails,default",
  #   "queue"=>"unbounce"}
  def manage_failed(klass: nil, queue: nil, action: :count, before: nil, after: nil)
    redis = Resque.redis
    key = 'failed'
    group_size = 100
    len = redis.llen(key)
    index = 0
    count = 0
    group_by_class = {}
    while index < len
      group = redis.lrange(key, index, [index+group_size-1, len-1].min)
      deleted = 0
      group.each do |encoded|
        data = Oj.load(encoded)
        next if klass && data['payload']['class'] != klass
        next if queue && data['queue'] != queue
        next if before && Time.parse(data['failed_at']) >= before
        next if after && Time.parse(data['failed_at']) < after

        count += 1
        if action == :delete
          redis.lrem(key, 1, encoded)
          deleted += 1
          len -= 1
        elsif action == :count_group
          c = data['payload']['class']
          group_by_class[c] ||= 0
          group_by_class[c] += 1
        end
      end
      index += group.size - deleted
      print "\r#{index}/#{len}, found #{count}"
    end

    puts " Done!"
    pp group_by_class.sort_by{|k,c| c } if action == :count_group
    nil
  end

  def count_delayed_by_class
    counts = Hash.new(0)
    manage_delayed { |job| counts[job['class']] += 1; false }
    pp(counts.sort_by { |_c, n| -n })
  end

  # This function process all delayed jobs and count, delete or change the job
  # Action is defined by the return of the block:
  #   Hash - will schedule the changed job instead of matched,
  #          there's no data validation here, so make sure to return correct job hash
  #   nil/false - nothing
  #   true - count as matched
  #   :delete - will delete the matched job
  def manage_delayed(log_file: nil)
    raise ArgumentError, 'Please supply a block' unless block_given?

    timestamps = redis.zrange(:delayed_queue_schedule, 0, -1)
    deleted = changed = matched = scanned = 0
    ts_total = timestamps.size
    logger = Logger.new(log_file) if log_file

    timestamps.each.with_index do |ts, ts_index|
      key = "delayed:#{ts}"
      redis.lrange(key, 0, -1).each do |job|
        scanned += 1
        res = yield(decode(job))
        matched += 1 if res
        case res
        when Hash
          change_delayed_job(key, job, encode(res))
          changed += 1
          logger&.info({action: 'change', job: job, new_job: new_job, ts: key}.to_json)
        when :delete
          delete_delayed_job(key, job)
          deleted += 1
          logger&.info({action: 'delete', job: job, ts: key}.to_json)
        when nil, false, true
          logger&.info({action: 'match', job: job, ts: key}.to_json)
        else
          raise "Invalid action: #{res.inspect}"
        end

        print "\r#{ts_index + 1}/#{ts_total}: scanned: #{scanned}, matched: #{matched}, deleted: #{deleted}, changed: #{changed}" if scanned % 1000 == 0
      end
    end

    puts "\r#{ts_total}/#{ts_total}: scanned: #{scanned}, matched: #{matched}, deleted: #{deleted}, changed: #{changed}" if scanned % 1000 == 0

    deleted
  end

  def analyze_delayed
    redis = Resque.redis
    timestamps = redis.zrange(:delayed_queue_schedule, 0, -1).map(&:to_i)
    timestamps.each.with_index do |ts, i|
      next if ts > new_ts

      jobs = redis.lrange("delayed:#{ts}", 0, -1)
      jobs.each do |job|
        data = Oj.load(job)
        remove_delayed_job(job) if data['class'] == '' && df
        counts[job['class']][ts.to_i] ||= 0
        counts[job['class']][ts.to_i] += 1
      end
    end

    counts
  end

  def delete_delayed_job(ts_key, job)
    redis.lrem(ts_key, 0, job)
    redis.srem("timestamps:#{job}", ts_key)
  end

  def add_delayed_job(ts_key, job)
    redis.lpush(ts_key, job)
    redis.sadd("timestamps:#{job}", ts_key)
  end

  def change_delayed_job(ts_key, old_job, new_job)
    delete_delayed_job(ts_key, old_job)
    add_delayed_job(ts_key, new_job)
  end

  def encode(job)
    Resque.encode(job)
  end

  def decode(encode_job)
    Resque.decode(encode_job)
  end

  def clear_status_by_time(ts_min, ts_max, filter_status = nil)
    ids = redis.zrangebyscore(Resque::Plugins::Status::Hash.set_key, ts_min, ts_max) || []
    return if ids.empty?

    if filter_status
      statuses = Resque::Plugins::Status::Hash.mget(ids).compact || []
      statuses = Resque::Plugins::Status::Hash.filter_statuses(statuses, status: filter_status)
      ids = statuses.map(&:uuid)
    end

    ids.each { |uuid| Resque::Plugins::Status::Hash.remove(uuid) }.size
  end
end
