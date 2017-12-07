using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Metrics.InfluxDB.Adapters;
using Metrics.InfluxDB.Model;
using Xunit;

namespace Metrics.InfluxDB.Tests
{
	public class InfluxdbConverterTests
	{
		[Fact]
		public void GetRecordsForCounter_ReturnsOneRecord()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment();
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(1);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt Count=1i ");
		}

		[Fact]
		public void GetRecordsForCounter_WithSetItem_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment("setItem");
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(2);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt setItem_Count=1i,setItem_Percent=100 ");
			records[1].ToLineProtocol().Should().StartWith("testcnt Count=1i ");
		}

		[Fact]
		public void GetRecordsForCounter_WithMultipleSetItems_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment("setItem");
			counter.Increment("setItem2", 3);
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(3);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt setItem_Count=1i,setItem_Percent=25 ");
			records[1].ToLineProtocol().Should().StartWith("testcnt setItem2_Count=3i,setItem2_Percent=75 ");
			records[2].ToLineProtocol().Should().StartWith("testcnt Count=4i ");
		}

		[Fact]
		public void GetRecordsForCounter_WithTags_ReturnsOneRecord()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = new Dictionary<string,string>();
		    tags.Add("key1", "value1");
		    tags.Add("tag2", "");
		    tags.Add("tag3", "");
		    tags.Add("key4", "value4");
			var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment();
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(1);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 Count=1i ");
		}

		[Fact]
		public void GetRecordsForCounter_WithTagsAndSetItem_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = new Dictionary<string,string>();
		    tags.Add("key1", "value1");
		    tags.Add("tag2", "");
		    tags.Add("tag3", "");
		    tags.Add("key4", "value4");
            var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment("setItem");
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(2);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 setItem_Count=1i,setItem_Percent=100 ");
			records[1].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 Count=1i ");
		}

		[Fact]
		public void GetRecordsForCounter_WithTagsAndMultipleSetItems_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

		    var tags = new Dictionary<string, string>();
		    tags.Add("key1", "value1");
		    tags.Add("tag2", "");
		    tags.Add("tag3", "");
		    tags.Add("key4", "value4");
            var counter = context.Counter("testcnt", Unit.Bytes, tags);

			counter.Increment("setItem");
			counter.Increment("setItem2", 3);
			metricsData = context.DataProvider.CurrentMetricsData;
			var counterSrc = metricsData.Counters.First();

			var records = converter.GetRecords("testcnt", tags, Unit.None, counterSrc.Value).ToList();
			records.Count().Should().Be(3);
			
			records[0].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 setItem_Count=1i,setItem_Percent=25 ");
			records[1].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 setItem2_Count=3i,setItem2_Percent=75 ");
			records[2].ToLineProtocol().Should().StartWith("testcnt,key1=value1,key4=value4 Count=4i ");
		}

		[Fact]
		public void GetRecordsForMeter_ReturnsOneRecord()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark();
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(1);
			
			records[0].ToLineProtocol().Should().StartWith(@"testmeter Count=1i,Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForMeter_WithSetItem_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark("setItem");
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(2);
			
			records[0].ToLineProtocol().Should().StartWith(@"testmeter setItem_Count=1i,setItem_Percent=100,setItem_Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",setItem_1\ Min\ Rate=0,setItem_5\ Min\ Rate=0,setItem_15\ Min\ Rate=0 ");

			records[1].ToLineProtocol().Should().StartWith(@"testmeter Count=1i,Mean\ Rate=");
			records[1].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForMeter_WithMultipleSetItems_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = default(MetricTags);
			var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark("setItem");
			meter.Mark("setItem2", 3);
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(3);
			
			records[0].ToLineProtocol().Should().StartWith(@"testmeter setItem_Count=1i,setItem_Percent=25,setItem_Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",setItem_1\ Min\ Rate=0,setItem_5\ Min\ Rate=0,setItem_15\ Min\ Rate=0 ");

			records[1].ToLineProtocol().Should().StartWith(@"testmeter setItem2_Count=3i,setItem2_Percent=75,setItem2_Mean\ Rate=");
			records[1].ToLineProtocol().Should().Contain(@",setItem2_1\ Min\ Rate=0,setItem2_5\ Min\ Rate=0,setItem2_15\ Min\ Rate=0 ");

			records[2].ToLineProtocol().Should().StartWith(@"testmeter Count=4i,Mean\ Rate=");
			records[2].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForMeter_WithTags_ReturnsOneRecord()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

		    var tags = new Dictionary<string, string>();
		    tags.Add("key1", "value1");
		    tags.Add("tag2", "");
		    tags.Add("tag3", "");
		    tags.Add("key4", "value4");
            var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark();
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(1);
			
			records[0].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 Count=1i,Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForMeter_WithTagsAndSetItem_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

		    var tags = new Dictionary<string, string>();
		    tags.Add("key1", "value1");
		    tags.Add("tag2", "");
		    tags.Add("tag3", "");
		    tags.Add("key4", "value4");
            var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark("setItem");
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(2);
			
			records[0].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 setItem_Count=1i,setItem_Percent=100,setItem_Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",setItem_1\ Min\ Rate=0,setItem_5\ Min\ Rate=0,setItem_15\ Min\ Rate=0 ");

			records[1].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 Count=1i,Mean\ Rate=");
			records[1].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForMeter_WithTagsAndMultipleSetItems_ReturnsMultipleRecords()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");
			var meter = context.Meter("testmeter", Unit.Bytes, TimeUnit.Seconds, tags);

			meter.Mark("setItem");
			meter.Mark("setItem2", 3);
			metricsData = context.DataProvider.CurrentMetricsData;
			var meterSrc = metricsData.Meters.First();

			var records = converter.GetRecords("testmeter", tags, Unit.None, meterSrc.Value).ToList();
			records.Count().Should().Be(3);

			records[0].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 setItem_Count=1i,setItem_Percent=25,setItem_Mean\ Rate=");
			records[0].ToLineProtocol().Should().Contain(@",setItem_1\ Min\ Rate=0,setItem_5\ Min\ Rate=0,setItem_15\ Min\ Rate=0 ");

			records[1].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 setItem2_Count=3i,setItem2_Percent=75,setItem2_Mean\ Rate=");
			records[1].ToLineProtocol().Should().Contain(@",setItem2_1\ Min\ Rate=0,setItem2_5\ Min\ Rate=0,setItem2_15\ Min\ Rate=0 ");

			records[2].ToLineProtocol().Should().StartWith(@"testmeter,key1=value1,key4=value4 Count=4i,Mean\ Rate=");
			records[2].ToLineProtocol().Should().Contain(@",1\ Min\ Rate=0,5\ Min\ Rate=0,15\ Min\ Rate=0 ");
		}

		[Fact]
		public void GetRecordsForEvent_WhenNoFieldSpecified_UsesDefaultTimestampAsField()
		{
			var converter = new DefaultConverter(); converter.Timestamp = DateTime.UtcNow;
			var context = new DefaultMetricsContext("ctxt");
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");
			var evnt = context.Event("testevnt", tags);

			evnt.Record();
			metricsData = context.DataProvider.CurrentMetricsData;
			var evntSrc = metricsData.Events.First();

			var records = converter.GetRecords("testevnt", tags, evntSrc.Value).ToList();
			records.Count().Should().Be(1);

			records[0].ToLineProtocol().Should().StartWith("testevnt,key1=value1,key4=value4 timestamp=");
		}
	}
}
