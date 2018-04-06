require 'active_record/base'

module TransactionRetry
  module ActiveRecord
    module Base
      def self.included(base)
        base.extend(ClassMethods)
        base.class_eval do
          class << self
            alias_method :transaction_without_retry, :transaction
          end
        end
        base.extend(OverrideMethods) if TransactionRetry.auto_retry
      end

      module OverrideMethods
        def transaction(*objects, &block)
          transaction_with_retry(*objects, &block)
        end
      end

      module ClassMethods
        def transaction_with_retry(*objects, &block)
          retry_count = 0

          opts = if objects.last.is_a? Hash
                   objects.last
                 else
                   {}
          end

          retry_on = [opts.delete(:retry_on)] if opts[:retry_on]
          retry_on ||= TransactionRetry.retry_on
          max_retries = opts.delete(:max_retries) || TransactionRetry.max_retries

          begin
            transaction_without_retry(*objects, &block)
          rescue ::ActiveRecord::StatementInvalid => error
            raise if retry_count >= max_retries
            raise if tr_in_nested_transaction?
            raise if retry_on.blank?

            found = false

            retry_on.each do |retry_error|
              if retry_error.is_a? String
                found = (error.try(:cause) && error.cause.class.name == retry_error) || error.class.name == retry_error
              end
              if retry_error.is_a? Class
                found = (error.try(:cause) && error.cause.class == retry_error) || error.class == retry_error
              end
              break if found
            end

            raise unless found

            retry_count += 1
            postfix = { 1 => 'st', 2 => 'nd', 3 => 'rd' }[retry_count] || 'th'

            Rails.logger.warn "Transaction Error! Retrying for the #{retry_count}-#{postfix} time..." if defined? Rails
            tr_exponential_pause(retry_count)
            retry
          end
        end

        private

        # Sleep 0, 1, 2, 4, ... seconds up to the TransactionRetry.max_retries.
        # Cap the sleep time at 32 seconds.
        # An ugly tr_ prefix is used to minimize the risk of method clash in the future.
        def tr_exponential_pause(count)
          seconds = TransactionRetry.wait_times[count - 1] || 32

          if TransactionRetry.fuzz
            fuzz_factor = [seconds * 0.25, 1].max

            seconds += rand * (fuzz_factor * 2) - fuzz_factor
          end

          sleep(seconds) if seconds > 0
        end

        # Returns true if we are in the nested transaction (the one with :requires_new => true).
        # Returns false otherwise.
        # An ugly tr_ prefix is used to minimize the risk of method clash in the future.
        def tr_in_nested_transaction?
          connection.open_transactions != 0
        end
      end
    end
  end
end

ActiveRecord::Base.send(:include, TransactionRetry::ActiveRecord::Base)
