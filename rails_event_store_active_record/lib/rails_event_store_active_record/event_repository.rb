require 'activerecord-import'

module RailsEventStoreActiveRecord
  class EventRepository
    POSITION_SHIFT = 1
    SERIALIZED_GLOBAL_STREAM_NAME = "all".freeze

    def initialize
      verify_correct_schema_present
      @repo_reader = EventRepositoryReader.new
    end

    def append_to_stream(events, stream, expected_version)
      add_to_stream(normalize_to_array(events), stream, expected_version, true) do |event|
        build_event_record(event).save!
        event.event_id
      end
    end

    def link_to_stream(event_ids, stream, expected_version)
      (normalize_to_array(event_ids) - Event.where(id: event_ids).pluck(:id)).each do |id|
        raise RubyEventStore::EventNotFound.new(id)
      end
      add_to_stream(normalize_to_array(event_ids), stream, expected_version, nil) do |event_id|
        event_id
      end
    end

    def delete_stream(stream)
      EventInStream.where(stream: stream.name).delete_all
    end

    def has_event?(event_id)
      @repo_reader.has_event?(event_id)
    end

    def last_stream_event(stream)
      @repo_reader.last_stream_event(stream)
    end

    def read_event(event_id)
      @repo_reader.read_event(event_id)
    end

    def read(specification)
      @repo_reader.read(specification)
    end

    private

    def add_to_stream(collection, stream, expected_version, include_global, &to_event_id)
      last_stream_version = ->(stream_) { EventInStream.where(stream: stream_.name).order("position DESC").first.try(:position) }
      resolved_version = expected_version.resolve_for(stream, last_stream_version)

      ActiveRecord::Base.transaction(requires_new: true) do
        in_stream = collection.flat_map.with_index do |element, index|
          position = compute_position(resolved_version, index)
          event_id = to_event_id.call(element)
          collection = []
          collection.unshift({
            stream: SERIALIZED_GLOBAL_STREAM_NAME,
            position: nil,
            event_id: event_id,
          }) if include_global
          collection.unshift({
            stream:   stream.name,
            position: position,
            event_id: event_id
          }) unless stream.global?
          collection
        end
        fill_ids(in_stream)
        EventInStream.import(in_stream)
      end
      self
    rescue ActiveRecord::RecordNotUnique => e
      raise_error(e)
    end

    def raise_error(e)
      if detect_index_violated(e.message)
        raise RubyEventStore::EventDuplicatedInStream
      end
      raise RubyEventStore::WrongExpectedEventVersion
    end

    def compute_position(resolved_version, index)
      unless resolved_version.nil?
        resolved_version + index + POSITION_SHIFT
      end
    end

    def detect_index_violated(message)
      IndexViolationDetector.new.detect(message)
    end

    def build_event_record(serialized_record)
      Event.new(
        id:         serialized_record.event_id,
        data:       serialized_record.data,
        metadata:   serialized_record.metadata,
        event_type: serialized_record.event_type
      )
    end

    def normalize_to_array(events)
      return events if events.is_a?(Enumerable)
      [events]
    end

    def verify_correct_schema_present
      CorrectSchemaVerifier.new.verify
    end

    # Overwritten in a sub-class
    def fill_ids(_in_stream)
    end
  end

end
