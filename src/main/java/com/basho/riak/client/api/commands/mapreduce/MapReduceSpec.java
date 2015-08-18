package com.basho.riak.client.api.commands.mapreduce;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * MapReduce Job Model / Specification
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
class MapReduceSpec
{

	private final MapReduceInput inputs;
	@JsonProperty(value = "query")
	private final List<MapReducePhase> phases;
	private final Long timeout;

	MapReduceSpec(MapReduceInput inputs, List<MapReducePhase> phases, Long timeout)
	{
		this.inputs = inputs;
		this.phases = phases;
		this.timeout = timeout;
	}

	public List<MapReducePhase> getPhases()
	{
		return phases;
	}

	public Long getTimeout()
	{
		return timeout;
	}

	public MapReduceInput getInputs()
	{
		return inputs;
	}
}
