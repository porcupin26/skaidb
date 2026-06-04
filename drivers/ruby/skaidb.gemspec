# frozen_string_literal: true

require_relative "lib/skaidb"

Gem::Specification.new do |spec|
  spec.name        = "skaidb"
  spec.version     = Skaidb::VERSION
  spec.license     = "SSPL-1.0"
  spec.summary     = "Official Ruby driver for skaidb (binary wire protocol, pg-style API)"
  spec.description = "A pure-stdlib Ruby client for skaidb. Speaks the binary " \
                     "wire protocol with SCRAM-SHA-256 auth and exposes a " \
                     "ruby-pg-style API (exec / exec_params, $1 placeholders, " \
                     "PG::Result-shaped results). No runtime dependencies."
  spec.authors     = ["skaidb"]
  spec.homepage    = "https://github.com/skaidb/skaidb"

  spec.required_ruby_version = ">= 2.7"

  spec.files = ["lib/skaidb.rb", "README.md"]
  spec.require_paths = ["lib"]

  spec.metadata["rubygems_mfa_required"] = "true"
end
