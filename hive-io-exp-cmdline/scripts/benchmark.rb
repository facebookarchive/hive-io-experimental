#!/usr/bin/env ruby
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if ARGV.empty?
  puts "Usage: #{__FILE__} <jar>"
  exit
end

jar = ARGV.shift
table = 'dim_friendlist'
date = '2013-04-15'
max_threads = 200

(1..max_threads).step(5) do |threads|
  args = [
    "tail",
    "--table '#{table}'",
    "--partitionFilter \"ds='#{date}'\"",
    "--threads #{threads}",
    "--append-benchmark-to bench-out.txt",
  ]
  system("java -jar #{jar} #{args.join(' ')}")
end
