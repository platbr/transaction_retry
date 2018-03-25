require "active_record"

require_relative "transaction_retry/version"

module TransactionRetry
  # Must be called after ActiveRecord established a connection.
  # Only then we know which connection adapter is actually loaded and can be enhanced.
  # Please note ActiveRecord does not load unused adapters.
  def self.apply_activerecord_patch
    require_relative 'transaction_retry/active_record/base'
    # ActiveRecord::Base.send(:include, TransactionRetry::ActiveRecord::Base)
  end

  if defined?( ::Rails )
    # Setup applying the patch after Rails is initialized.
    class Railtie < ::Rails::Railtie
      config.after_initialize do
        TransactionRetry.apply_activerecord_patch
      end
    end
  end

  def self.auto_rety=( val )
    @@auto_rety = val
  end

  def self.auto_rety
    @@auto_rety ||= false
  end

  def self.retry_on=( value )
    @@retry_on ||= [PG::TRSerializationFailure, PG::TRDeadlockDetected]
  end

  def self.retry_on
    @@retry_on ||= nil
  end

  def self.max_retries
    @@max_retries ||= 3
  end

  def self.max_retries=( n )
    @@max_retries = n
  end

  def self.wait_times
    @@wait_times ||= [0, 1, 2, 4, 8, 16, 32]
  end

  def self.wait_times=( array_of_seconds )
    @@wait_times = array_of_seconds
  end

  def self.fuzz
    @@fuzz ||= true
  end

  def self.fuzz=( val )
    @@fuzz = val
  end

end
