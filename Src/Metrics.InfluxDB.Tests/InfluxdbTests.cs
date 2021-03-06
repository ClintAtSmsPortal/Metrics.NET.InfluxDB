﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Metrics.InfluxDB.Model;
using FluentAssertions;
using Xunit;

namespace Metrics.InfluxDB.Tests
{
	public class InfluxdbTests
	{
		private readonly Uri mockUri = new Uri("https://localhost");

		[Fact]
		public void InfluxTag_CanParse_InvalidValueReturnsEmpty() {
			// invalid input strings
			InfluxTag empty = InfluxTag.Empty;
			String nullReason = "Because the input string should contain a single key and value separated by an equals sign.";
			
			
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>("key", "")).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>(null, "value")).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>(null, "")).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>(null, null)).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>("", "value")).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>("", null)).Should().Be(empty, nullReason);
			InfluxUtils.ToInfluxTag(new KeyValuePair<string, string>("", "")).Should().Be(empty, nullReason);
		}



		[Fact]
		public void InfluxField_SupportsValidValueTypes() {
			var validTypes = InfluxUtils.ValidValueTypes;
			foreach (var type in validTypes)
				InfluxUtils.IsValidValueType(type).Should().BeTrue();
		}

		[Theory]
		[MemberData(nameof(TagTestCasesArray))]
		public void InfluxTag_FormatsTo_LineProtocol(InfluxTag tag, String output) {
			tag.ToLineProtocol().Should().Be(output);
			tag.ToString().Should().Be(output);
		}

		[Theory]
		[MemberData(nameof(FieldTestCasesArray))]
		public void InfluxField_FormatsTo_LineProtocol(InfluxField field, String output) {
			field.ToLineProtocol().Should().Be(output);
			field.ToString().Should().Be(output);
		}

		[Fact]
		public void InfluxRecord_FormatsTo_LineProtocol() {
			// test values
			var testNow = new DateTime(2016, 6, 1, 0, 0, 0, DateTimeKind.Utc);
			var testTags = TagTestCases.Select(tc => tc.Tag);
			var testFields = FieldTestCases.Select(tc => tc.Field);
			var precision = InfluxConfig.Default.Precision;

			// expected values
			String expTime = InfluxLineProtocol.FormatTimestamp(testNow, precision);
			String expTags = String.Join(",", TagTestCases.Select(tc => tc.Output));
			String expFields = String.Join(",", FieldTestCases.Select(tc => tc.Output));
			String expOutput = String.Format("test_name,{0} {1} {2}", expTags, expFields, expTime);

			// assert line values match expected
			new InfluxRecord("name spaces", new[] { new InfluxField("field1", 123456) })
				.ToLineProtocol(precision).Should().Be(@"name\ spaces field1=123456i");
			new InfluxRecord("test_name", new[] { new InfluxTag("tag1", "value1") }, new[] { new InfluxField("field1", 123456) })
				.ToLineProtocol(precision).Should().Be(@"test_name,tag1=value1 field1=123456i");
			new InfluxRecord("test_name", new[] { new InfluxTag("tag1", "value1"), new InfluxTag("tag2", "value2") }, new[] { new InfluxField("field1", 123456), new InfluxField("field2", true) })
				.ToLineProtocol(precision).Should().Be(@"test_name,tag1=value1,tag2=value2 field1=123456i,field2=True");
			new InfluxRecord("test_name", new[] { new InfluxTag("tag1", "value1") }, new[] { new InfluxField("field1", "test string") }, testNow)
				.ToLineProtocol(precision).Should().Be($@"test_name,tag1=value1 field1=""test string"" {expTime}");
			new InfluxRecord("test_name", testTags, testFields, testNow)
				.ToLineProtocol(precision).Should().Be(expOutput);
		}

		[Fact]
		public void InfluxBatch_FormatsTo_LineProtocol() {
			var testNow = new DateTime(2016, 6, 1, 0, 0, 0, DateTimeKind.Utc);
			var testTags = TagTestCases.Select(tc => tc.Tag);
			var testFields = FieldTestCases.Select(tc => tc.Field);
			var precision = InfluxConfig.Default.Precision;
			var expTime = InfluxLineProtocol.FormatTimestamp(testNow, precision);

			// test with empty batch
			InfluxBatch batch = new InfluxBatch();
			batch.ToLineProtocol(precision).Should().BeEmpty();

			// test with single record
			batch.Add(new InfluxRecord("test_name", new[] { new InfluxTag("tag1", "value1") }, new[] { new InfluxField("field1", 123456) }));
			batch.ToLineProtocol(precision).Should().NotEndWith("\n").And.Be(@"test_name,tag1=value1 field1=123456i");
			batch.Clear();

			// test with multiple records
			batch.Add(new InfluxRecord("test_name1", new[] { new InfluxTag("tag1", "value1") }, new[] { new InfluxField("field1", 123456) }));
			batch.Add(new InfluxRecord("test_name2", new[] { new InfluxTag("tag2", "value2") }, new[] { new InfluxField("field2", 234561) }));
			batch.Add(new InfluxRecord("test_name3", new[] { new InfluxTag("tag3", "value3") }, new[] { new InfluxField("field3", 345612) }, testNow));
			batch.Add(new InfluxRecord("test_name4", new[] { new InfluxTag("tag4", "value4") }, new[] { new InfluxField("field4", 456123) }, testNow));
			batch.Add(new InfluxRecord("test_name5", new[] { new InfluxTag("tag5", "value5") }, new[] { new InfluxField("field5", 561234) }, testNow));

			String expOutput = String.Join("\n",
				$@"test_name1,tag1=value1 field1=123456i",
				$@"test_name2,tag2=value2 field2=234561i",
				$@"test_name3,tag3=value3 field3=345612i {expTime}",
				$@"test_name4,tag4=value4 field4=456123i {expTime}",
				$@"test_name5,tag5=value5 field5=561234i {expTime}"
			);

			batch.ToLineProtocol(precision).Should().NotEndWith("\n").And.Be(expOutput);
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForGauge() {
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;

			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().BeEmpty("Because running a report with no metrics should not result in any records.");

			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");
			context.Gauge("test_gauge", () => 123.456, Unit.Bytes, tags);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"testcontext.test_gauge.gauge,key1=value1,key4=value4 value=123.456 {expTime}");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForCounter() {
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;
			var tags = new Dictionary<string,string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");
			var counter = context.Counter("test_counter", Unit.Bytes, tags);

			// add normally
			counter.Increment(300);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);
			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"testcontext.test_counter.counter,key1=value1,key4=value4 count=300i {expTime}");

			// add with set item
			counter.Increment("item1", 100);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(2);

			expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"testcontext.test_counter.counter,key1=value1,key4=value4 item1_count=100i,item1_percent=25 {expTime}");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForMeter() {
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;
			var tags =new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");

			var meter = context.Meter("test_meter", Unit.Bytes, TimeUnit.Seconds, tags);

			// add normally
			meter.Mark(300);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().StartWith($@"testcontext.test_meter.meter,key1=value1,key4=value4 count=300i,mean_rate=").And.EndWith($@",1_min_rate=0,5_min_rate=0,15_min_rate=0 {expTime}");

			// add with set item
			meter.Mark("item1", 100);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(2);

			var lastBatch = writer.LastBatch[0].ToLineProtocol(precision);
			lastBatch.Should().StartWith($@"testcontext.test_meter.meter,key1=value1,key4=value4 item1_count=100i,item1_percent=25,item1_mean_rate=");
			expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			lastBatch.Should().EndWith($@",item1_1_min_rate=0,item1_5_min_rate=0,item1_15_min_rate=0 {expTime}");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForHistogram() {
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;
			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");

			var hist = context.Histogram("test_hist", Unit.Bytes, SamplingType.Default, tags);

			// add normally
			hist.Update(300);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"testcontext.test_hist.histogram,key1=value1,key4=value4 count=1i,last=300,min=300,mean=300,max=300,stddev=0,median=300,sample_size=1i,percentile_75%=300,percentile_95%=300,percentile_98%=300,percentile_99%=300,percentile_99.9%=300 {expTime}");

			// add with set item
			hist.Update(100, "item1");
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"testcontext.test_hist.histogram,key1=value1,key4=value4 count=2i,last=100,min=100,mean=200,max=300,stddev=100,median=300,sample_size=2i,percentile_75%=300,percentile_95%=300,percentile_98%=300,percentile_99%=300,percentile_99.9%=300 {expTime}");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForTimer()
		{
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;
			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");

			var timer = context.Timer("test_timer", Unit.Bytes, SamplingType.Default, TimeUnit.Seconds, TimeUnit.Seconds, tags);

			// add normally
			timer.Record(100, TimeUnit.Seconds);
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().StartWith($@"testcontext.test_timer.timer,key1=value1,key4=value4 active_sessions=0i,total_time=100i,count=1i,").And.EndWith($@",1_min_rate=0,5_min_rate=0,15_min_rate=0,last=100,min=100,mean=100,max=100,stddev=0,median=100,sample_size=1i,percentile_75%=100,percentile_95%=100,percentile_98%=100,percentile_99%=100,percentile_99.9%=100 {expTime}");

			// add with set item
			timer.Record(50, TimeUnit.Seconds, "item1");
			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(1);

			var lastBatch = writer.LastBatch[0].ToLineProtocol(precision);
			lastBatch.Should().StartWith($@"testcontext.test_timer.timer,key1=value1,key4=value4 active_sessions=0i,total_time=150i,count=2i,mean_rate=");
			expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			lastBatch.Should().Contain(",1_min_rate=0,5_min_rate=0,15_min_rate=0,last=50,min=50,mean=");
			lastBatch.Should().Contain(",max=100,stddev=");
			lastBatch.Should().EndWith($",median=100,sample_size=2i,percentile_75%=100,percentile_95%=100,percentile_98%=100,percentile_99%=100,percentile_99.9%=100 {expTime}");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForEvent()
		{
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;
			var tags = new Dictionary<string, string>();
			tags.Add("key1", "value1");
			tags.Add("tag2", "");
			tags.Add("tag3", "");
			tags.Add("key4", "value4");

			var evnt = context.Event("test_evnt", tags);

			var fields = new Dictionary<string, object>();
			fields.Add("stringTag", "abc");
			fields.Add("intTag", 10);
			fields.Add("longTag", 10l);
			fields.Add("doubleTag", 10.1d);
			fields.Add("floatTag", 1.0f);
			fields.Add("decimalTag", 12.0m);
			fields.Add("byteTag", (byte)11);
			fields.Add("boolTag", true);
			evnt.Record(fields);

			fields = new Dictionary<string, object>();
			fields.Add("stringTag", "xyz");
			evnt.Record(fields);

			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, hsFunc, CancellationToken.None);
			writer.LastBatch.Should().HaveCount(2);

			writer.LastBatch[0].ToLineProtocol(precision).Should().StartWith("testcontext.test_evnt.event,key1=value1,key4=value4 stringtag=\"abc\",inttag=10i,longtag=10i,doubletag=10.1,floattag=1,decimaltag=12,bytetag=11i,booltag=True");
			writer.LastBatch[1].ToLineProtocol(precision).Should().StartWith("testcontext.test_evnt.event,key1=value1,key4=value4 stringtag=\"xyz\"");
		}

		[Fact]
		public void InfluxReport_CanAddRecords_ForHealthCheck() {
			var config = new InfluxConfig(mockUri, "testdb");
			var writer = new InfluxdbTestWriter(config); config.Writer = writer;
			var report = new InfluxdbHttpReport(config);
			var context = new DefaultMetricsContext("TestContext");
			var precision = config.Precision ?? InfluxConfig.Default.Precision;
			var metricsData = context.DataProvider.CurrentMetricsData;

			var tempTags3 = new Dictionary<string,string>();
			tempTags3.Add("tag3", "key3");
			var tempTags4 = new Dictionary<string, string>();
			tempTags4.Add("tag 4", "key 4");
			var tempTags5 = new Dictionary<string, string>();
			tempTags5.Add("tag5", "key5");

			HealthChecks.UnregisterAllHealthChecks();
			HealthChecks.RegisterHealthCheck("Health Check 1", () => HealthCheckResult.Healthy($"Healthy check!"));
			HealthChecks.RegisterHealthCheck("Health Check 2", () => HealthCheckResult.Unhealthy($"Unhealthy check!"));
			HealthChecks.RegisterHealthCheck("Health Check 3", () => HealthCheckResult.Healthy($"Healthy check!"), tempTags3);
			HealthChecks.RegisterHealthCheck("Health Check 4",	() => HealthCheckResult.Healthy($"Healthy check!"), tempTags4);
			HealthChecks.RegisterHealthCheck("Name=Health Check 5", () => HealthCheckResult.Healthy($"Healthy check!"), tempTags5);

			metricsData = context.DataProvider.CurrentMetricsData;
			report.RunReport(metricsData, () => HealthChecks.GetStatus(), CancellationToken.None);
			HealthChecks.UnregisterAllHealthChecks(); // unreg first in case something below throws
			writer.LastBatch.Should().HaveCount(5);

			var expTime = InfluxLineProtocol.FormatTimestamp(metricsData.Timestamp, precision);
			writer.LastBatch[0].ToLineProtocol(precision).Should().Be($@"health_checks,name=health_check_1 ishealthy=True,message=""Healthy check!"" {expTime}");
			writer.LastBatch[1].ToLineProtocol(precision).Should().Be($@"health_checks,name=health_check_2 ishealthy=False,message=""Unhealthy check!"" {expTime}");
			writer.LastBatch[2].ToLineProtocol(precision).Should().Be($@"health_checks,name=health_check_3,tag3=key3 ishealthy=True,message=""Healthy check!"" {expTime}");
			writer.LastBatch[3].ToLineProtocol(precision).Should().Be($@"health_checks,name=health_check_4,tag_4=key\ 4 ishealthy=True,message=""Healthy check!"" {expTime}");
			writer.LastBatch[4].ToLineProtocol(precision).Should().Be($@"health_checks,name=health_check_5,tag5=key5 ishealthy=True,message=""Healthy check!"" {expTime}");
		}



		#region Tag and Field Test Cases and Other Static Members

		public static IEnumerable<TagTestCase> TagTestCases = new[] {
			new TagTestCase("key1", "value1", @"key1=value1"),
			new TagTestCase("key2 with spaces", "value2 with spaces", @"key2\ with\ spaces=value2\ with\ spaces"),
			new TagTestCase("key3,with,commas", "value3,with,commas", @"key3\,with\,commas=value3\,with\,commas"),
			new TagTestCase("key4=with=equals", "value4=with=equals", @"key4\=with\=equals=value4\=with\=equals"),
			new TagTestCase("key5\"with\"quot", "value5\"with\"quot", "key5\"with\"quot=value5\"with\"quot"),
			new TagTestCase("key6\" with,all=", "value6\" with,all=", @"key6""\ with\,all\==value6""\ with\,all\="),
		};

		public static IEnumerable<FieldTestCase> FieldTestCases = new[] {
			new FieldTestCase("field1_int1",  100, @"field1_int1=100i"),
			new FieldTestCase("field1_int2", -100, @"field1_int2=-100i"),
			new FieldTestCase("field2_double1",  123456789.123456, @"field2_double1=123456789.123456"),
			new FieldTestCase("field2_double2", -123456789.123456, @"field2_double2=-123456789.123456"),
			new FieldTestCase("field2_double3", Math.PI, @"field2_double3=3.1415926535897931"),
			new FieldTestCase("field2_double4", Double.MinValue, @"field2_double4=-1.7976931348623157E+308"),
			new FieldTestCase("field2_double5", Double.MaxValue, @"field2_double5=1.7976931348623157E+308"),
			new FieldTestCase("field3_bool1", true,  @"field3_bool1=True"),
			new FieldTestCase("field3_bool2", false, @"field3_bool2=False"),
			new FieldTestCase("field4_string1", "string value1",  @"field4_string1=""string value1"""),
			new FieldTestCase("field4_string2", "string\"value2", @"field4_string2=""string\""value2"""),
			new FieldTestCase("field5 spaces", 100, @"field5\ spaces=100i"),
			new FieldTestCase("field6,commas", 100, @"field6\,commas=100i"),
			new FieldTestCase("field7=equals", 100, @"field7\=equals=100i"),
			new FieldTestCase("field8\"quote", 100, @"field8""quote=100i"),
		};

		// these must be defined after the above are defined so the variables are not null
		public static IEnumerable<Object[]> TagTestCasesArray = TagTestCases.Select(t => t.ToArray());
		public static IEnumerable<Object[]> FieldTestCasesArray = FieldTestCases.Select(t => t.ToArray());


		private static readonly Func<HealthStatus> hsFunc = () => new HealthStatus();

		#endregion

	}
}
