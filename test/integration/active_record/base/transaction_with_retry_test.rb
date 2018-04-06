# -*- encoding : utf-8 -*-

require 'test_helper'

class TransactionWithRetryTest < MiniTest::Test
  def setup
    @original_retry_on = TransactionRetry.retry_on
    @original_max_retries = TransactionRetry.max_retries
    @original_wait_times = TransactionRetry.wait_times
  end

  def teardown
    TransactionRetry.retry_on = @original_retry_on
    TransactionRetry.max_retries = @original_max_retries
    TransactionRetry.wait_times = @original_wait_times
    QueuedJob.delete_all
  end
  
  def test_does_not_break_transaction
    ActiveRecord::Base.transaction_with_retry do
      QueuedJob.create!( :job => 'is fun!' )
      assert_equal( 1, QueuedJob.count )
    end
    assert_equal( 1, QueuedJob.count )
    QueuedJob.first.destroy
  end

  def test_does_not_break_transaction_rollback
    ActiveRecord::Base.transaction_with_retry do
      QueuedJob.create!( :job => 'gives money!' )
      raise ActiveRecord::Rollback
    end
    assert_equal( 0, QueuedJob.count )
  end

  def test_retries_transaction_on_transaction_isolation_conflict
    first_run = true

    ActiveRecord::Base.transaction_with_retry do
      QueuedJob.create!( :job => 'is cool!' )
      if first_run
        first_run = false
        begin
          message = "Simulating a TRSerializationFailure"
          raise PG::TRSerializationFailure.new( message )
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end
    assert_equal( 1, QueuedJob.count )

    QueuedJob.first.destroy
  end

  def test_does_not_retry_on_unknown_error
    assert_raises(::ActiveRecord::StatementInvalid) do
      ActiveRecord::Base.transaction_with_retry do
        QueuedJob.create!( :job => 'is cool!' )
        begin
          raise StandardError.new("random error")
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end
    assert_equal( 0, QueuedJob.count )
  end

  def test_retries_on_custom_error
    first_run = true

    ActiveRecord::Base.transaction_with_retry(retry_on: StandardError) do
      QueuedJob.create!( :job => 'is cool!' )
      if first_run
        first_run = false
        begin
          raise StandardError.new("CustomError error")
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end

    assert_equal( 1, QueuedJob.count )

    ActiveRecord::Base.transaction_with_retry(retry_on: 'StandardError') do
      QueuedJob.create!( :job => 'is cool!' )
      if first_run
        first_run = false
        begin
          raise StandardError.new("CustomError error")
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end
    assert_equal( 2, QueuedJob.count )
    QueuedJob.all.destroy_all
  end

  def test_does_not_retry_transaction_more_than_max_retries_times
    TransactionRetry.max_retries = 1
    run = 0

    assert_raises( ::ActiveRecord::StatementInvalid ) do
      ActiveRecord::Base.transaction_with_retry do
        run += 1
        begin
          message = "Simulating a TRSerializationFailure"
          raise PG::TRSerializationFailure.new( message )
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end

    assert_equal( 2, run )  # normal run + one retry
  end

  def test_does_allow_override_default_max_retries
    TransactionRetry.max_retries = 3

    run = 0

    assert_raises(::ActiveRecord::StatementInvalid) do
      ActiveRecord::Base.transaction_with_retry(max_retries: 2) do
        run += 1
        begin
          message = "Simulating a TRSerializationFailure"
          raise PG::TRSerializationFailure.new(message)
        rescue => err
          raise ::ActiveRecord::StatementInvalid.new(err.message)
        end
      end
    end

    assert_equal( 3, run )  # normal run + 2 retry
  end

  def test_does_not_retry_nested_transaction
    first_try = true

    ActiveRecord::Base.transaction_with_retry do

      assert_raises( ::ActiveRecord::StatementInvalid ) do
        ActiveRecord::Base.transaction( :requires_new => true ) do
          if first_try
            first_try = false
            begin
              message = "Simulating a TRSerializationFailure"
              raise PG::TRSerializationFailure.new( message )
            rescue => err
              raise ::ActiveRecord::StatementInvalid.new(err.message)
            end
          end
          QueuedJob.create!( :job => 'is cool!' )
        end
      end
      
    end
      
    assert_equal( 0, QueuedJob.count )
  end

end
