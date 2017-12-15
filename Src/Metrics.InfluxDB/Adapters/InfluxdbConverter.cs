using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Metrics.InfluxDB.Model;
using Metrics.MetricData;

namespace Metrics.InfluxDB.Adapters
{
	/// <summary>
	/// This class converts Metrics.NET metric values into <see cref="InfluxRecord"/> objects.
	/// </summary>
	public abstract class InfluxdbConverter
	{
		/// <summary>
		/// Gets or sets the current timestamp. This value is used when creating new <see cref="InfluxRecord"/> instances.
		/// </summary>
		public DateTime? Timestamp { get; set; }

		/// <summary>
		/// Gets or sets the global tags. Global tags are added to all created <see cref="InfluxRecord"/> instances.
		/// </summary>
		public MetricTags GlobalTags { get; set; }


		/// <summary>
		/// Creates a new <see cref="InfluxdbConverter"/> using the default precision defined by <see cref="InfluxConfig.Default.Precision"/>.
		/// </summary>
		public InfluxdbConverter()
			: this(null)
		{
		}

		/// <summary>
		/// Creates a new <see cref="InfluxdbConverter"/> using the specified precision and tags.
		/// </summary>
		/// <param name="globalTags">The global tags that are added to all created <see cref="InfluxRecord"/> instances.</param>
		public InfluxdbConverter(MetricTags? globalTags = null)
		{
			GlobalTags = globalTags ?? MetricTags.None;
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> instance for the gauge value.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="unit">The metric unit.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, Unit unit, Double value)
		{
			yield return GetRecord(name, tags, new[] {
				new InfluxField("Value", value),
			});
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> instance for the counter value and any set items.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="unit">The metric unit.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, Unit unit, CounterValue value)
		{
			var setItems = new List<SetItem>();
			foreach (var i in value.Items)
			{
				var itemName = string.IsNullOrWhiteSpace(i.Item) ? string.Empty : i.Item;
				var setItem = new SetItem(itemName);
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.Count, i.Count));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.Percent, i.Percent));
				setItems.Add(setItem);
			}
			
			foreach (var item in setItems)
			{
				var tmpFields = new List<InfluxField>();
				foreach (var field in item.Fields)
				{
					tmpFields.Add(field);
				}
				var tmpJtags = InfluxUtils.JoinTags(GlobalTags, tags);
				var tmpItemTags = tmpJtags.ToDictionary(t=>t.Key,t=>t.Value);
				yield return GetRecord(name, new MetricTags(tmpItemTags), tmpFields);
			}
			
			var fields = new List<InfluxField>();
			fields.Add(new InfluxField("Count", value.Count));
			var jtags = InfluxUtils.JoinTags(GlobalTags, tags);
			var itemTags = jtags.ToDictionary(t => t.Key, t => t.Value);
			yield return GetRecord(name, new MetricTags(itemTags), fields);
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> instance for the meter value.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="unit">The metric unit.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, Unit unit, MeterValue value)
		{
			if (value == null) throw new ArgumentNullException(nameof(value));

			var setItems = new List<SetItem>();
			foreach (var i in value.Items)
			{
				var itemName = string.IsNullOrWhiteSpace(i.Item) ? string.Empty : i.Item;
				var setItem = new SetItem(itemName);
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.Count, i.Value.Count));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.Percent, i.Percent));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.MeanRate, i.Value.MeanRate));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.OneMinRate, i.Value.OneMinuteRate));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.FiveMinRate, i.Value.FiveMinuteRate));
				setItem.Fields.Add(new InfluxField(itemName + SetItem.Type.FifteenMinRate, i.Value.FifteenMinuteRate));
				setItems.Add(setItem);
			}
			
			foreach (var item in setItems)
			{
				var tmpFields = new List<InfluxField>();
				foreach (var field in item.Fields)
				{
					tmpFields.Add(field);
				}
				var tmpJtags = InfluxUtils.JoinTags(GlobalTags, tags);
				var tmpItemTags = tmpJtags.ToDictionary(t => t.Key, t => t.Value);
				yield return GetRecord(name, new MetricTags(tmpItemTags), tmpFields);
			}

			var fields = new List<InfluxField>();
			fields.Add(new InfluxField("Count", value.Count));
			fields.Add(new InfluxField("Mean Rate", value.MeanRate));
			fields.Add(new InfluxField("1 Min Rate", value.OneMinuteRate));
			fields.Add(new InfluxField("5 Min Rate", value.FiveMinuteRate));
			fields.Add(new InfluxField("15 Min Rate", value.FifteenMinuteRate));
			var jtags = InfluxUtils.JoinTags(GlobalTags, tags);
			var itemTags = jtags.ToDictionary(t => t.Key, t => t.Value);
			yield return GetRecord(name, new MetricTags(itemTags), fields);
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> instance for the histogram value.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="unit">The metric unit.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, Unit unit, HistogramValue value)
		{
			if (value == null) throw new ArgumentNullException(nameof(value));

			yield return GetRecord(name, tags, new[] {
				new InfluxField("Count",			value.Count),
				new InfluxField("Last",		  value.LastValue),
				new InfluxField("Min",			value.Min),
				new InfluxField("Mean",		  value.Mean),
				new InfluxField("Max",			value.Max),
				new InfluxField("StdDev",		  value.StdDev),
				new InfluxField("Median",		  value.Median),
				new InfluxField("Sample Size",	value.SampleSize),
				new InfluxField("Percentile 75%",   value.Percentile75),
				new InfluxField("Percentile 95%",   value.Percentile95),
				new InfluxField("Percentile 98%",   value.Percentile98),
				new InfluxField("Percentile 99%",   value.Percentile99),
				new InfluxField("Percentile 99.9%", value.Percentile999),

				// ignored histogram values
				//new InfluxField("Last User Value",  value.LastUserValue),
				//new InfluxField("Min User Value",   value.MinUserValue),
				//new InfluxField("Max User Value",   value.MaxUserValue),
			});
		}

		/// <summary>
		/// Creates new <see cref="InfluxRecord"/> instances for the timer values and any items in the meter item sets.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="unit">The metric unit.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, Unit unit, TimerValue value)
		{
			if (value == null) throw new ArgumentNullException(nameof(value));

			var fields = new List<InfluxField>();
			fields.Add(new InfluxField("Active Sessions", value.ActiveSessions));
			fields.Add(new InfluxField("Total Time", value.TotalTime));
			fields.Add(new InfluxField("Count", value.Rate.Count));
			fields.Add(new InfluxField("Mean Rate", value.Rate.MeanRate));
			fields.Add(new InfluxField("1 Min Rate", value.Rate.OneMinuteRate));
			fields.Add(new InfluxField("5 Min Rate", value.Rate.FiveMinuteRate));
			fields.Add(new InfluxField("15 Min Rate", value.Rate.FifteenMinuteRate));
			fields.Add(new InfluxField("Last", value.Histogram.LastValue));
			fields.Add(new InfluxField("Min", value.Histogram.Min));
			fields.Add(new InfluxField("Mean", value.Histogram.Mean));
			fields.Add(new InfluxField("Max", value.Histogram.Max));
			fields.Add(new InfluxField("StdDev", value.Histogram.StdDev));
			fields.Add(new InfluxField("Median", value.Histogram.Median));

			fields.Add(new InfluxField("Sample Size", value.Histogram.SampleSize));
			fields.Add(new InfluxField("Percentile 75%", value.Histogram.Percentile75));
			fields.Add(new InfluxField("Percentile 95%", value.Histogram.Percentile95));
			fields.Add(new InfluxField("Percentile 98%", value.Histogram.Percentile98));
			fields.Add(new InfluxField("Percentile 99%", value.Histogram.Percentile99));
			fields.Add(new InfluxField("Percentile 99.9%", value.Histogram.Percentile999));
			// ignored histogram values
			//fields.Add(new InfluxField("Last User Value",  value.Histogram.LastUserValue));
			//fields.Add(new InfluxField("Min User Value",   value.Histogram.MinUserValue));
			//fields.Add(new InfluxField("Max User Value",   value.Histogram.MaxUserValue));
			
			var jtags = InfluxUtils.JoinTags(GlobalTags, tags);
			var itemTags = jtags.ToDictionary(t => t.Key, t => t.Value);
			yield return GetRecord(name, new MetricTags(itemTags), fields);
		}

		/// <summary>
		/// Creates new <see cref="InfluxRecord"/> instances for each HealthCheck result in the specified <paramref name="status"/>.
		/// </summary>
		/// <param name="status">The health status.</param>
		/// <returns></returns>
		public IEnumerable<InfluxRecord> GetRecords(HealthStatus status)
		{
			var setItemTags = new List<KeyValuePair<string, string>>();
			foreach (var result in status.Results)
			{
				var itemName = string.IsNullOrWhiteSpace(result.Name) ? string.Empty : result.Name;
				Dictionary<string,string> nameTag = new Dictionary<string, string>();
				if (!Regex.IsMatch(itemName, "^[Nn]ame="))
				{
					nameTag.Add("name", InfluxUtils.LowerAndReplaceSpaces(itemName));
				}
				else
				{
					nameTag.Add("name", InfluxUtils.LowerAndReplaceSpaces(itemName.Substring(5)));
				}

				var jtags = InfluxUtils.JoinTags(GlobalTags, result.Tags, nameTag);
				var itemTags = jtags.ToDictionary(t=> t.Key,t=> t.Value);
				yield return GetRecord("Health Checks", itemTags, new[] {
					new InfluxField("IsHealthy", result.Check.IsHealthy),
					new InfluxField("Message",   result.Check.Message)
				});
			}
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> instance for the event value.
		/// </summary>
		/// <param name="name">The measurement name.</param>
		/// <param name="tags">Any additional tags to add to the <see cref="InfluxRecord"/>, these tags overwrite any global tags with the same name.</param>
		/// <param name="value">The metric value object.</param>
		/// <returns>A list of <see cref="InfluxRecord"/> instances for the specified metric value.</returns>
		public IEnumerable<InfluxRecord> GetRecords(String name, MetricTags tags, EventValue value)
		{
			foreach (var evntArgs in value.Events)
			{
				var fields = new List<InfluxField>();
				foreach (var kvp in evntArgs.Fields)
				{
					fields.Add(new InfluxField(kvp.Key, kvp.Value));
				}
				yield return GetRecord(name, tags, fields, evntArgs.Timestamp);
			}
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> from the specified name, tags, and fields.
		/// This uses the timestamp defined on this metrics converter instance.
		/// </summary>
		/// <param name="name">The measurement or series name. This value is required and cannot be null or empty.</param>
		/// <param name="tags">The optional tags to associate with this record.</param>
		/// <param name="fields">The <see cref="InfluxField"/> values for the output fields.</param>
		/// <returns>A new <see cref="InfluxRecord"/> from the specified name, tags, and fields.</returns>
		public InfluxRecord GetRecord(String name, MetricTags tags, IEnumerable<InfluxField> fields)
		{
			return GetRecord(name, null, tags, fields);
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> from the specified name, item name, tags, and fields.
		/// This uses the timestamp defined on this metrics converter instance.
		/// </summary>
		/// <param name="name">The measurement or series name. This value is required and cannot be null or empty.</param>
		/// <param name="itemName">The set item name. Can contain comma-separated key/value pairs.</param>
		/// <param name="tags">The optional tags to associate with this record.</param>
		/// <param name="fields">The <see cref="InfluxField"/> values for the output fields.</param>
		/// <returns>A new <see cref="InfluxRecord"/> from the specified name, tags, and fields.</returns>
		public InfluxRecord GetRecord(String name, String itemName, MetricTags tags, IEnumerable<InfluxField> fields)
		{
			var jtags = InfluxUtils.JoinTags(itemName, GlobalTags, tags); // global tags must be first so they can get overridden
			var record = new InfluxRecord(name, jtags, fields, Timestamp);
			return record;
		}

		/// <summary>
		/// Creates a new <see cref="InfluxRecord"/> from the specified name, item name, tags, fields, and timestamp.
		/// </summary>
		/// <param name="name">The measurement or series name. This value is required and cannot be null or empty.</param>
		/// <param name="tags">The optional tags to associate with this record.</param>
		/// <param name="fields">The <see cref="InfluxField"/> values for the output fields.</param>
		/// <param name="timestamp">The timestamp for the <see cref="InfluxRecord"/>.</param>
		/// <returns>A new <see cref="InfluxRecord"/> from the specified name, tags, and fields.</returns>
		public InfluxRecord GetRecord(String name, MetricTags tags, IEnumerable<InfluxField> fields, DateTime timestamp)
		{
			var jtags = InfluxUtils.JoinTags((string)null, GlobalTags, tags); // global tags must be first so they can get overridden
			var record = new InfluxRecord(name, jtags, fields, timestamp);
			return record;
		}

		private class SetItem
		{
			public string Name { get; set; }

			public readonly List<InfluxField> Fields = new List<InfluxField>();

			public SetItem(string name)
			{
				this.Name = name;
			}

			public static class Type
			{
				public static string Count { get { return "_Count"; } }
				public static string Percent { get { return "_Percent"; } }
				public static string MeanRate { get { return "_Mean Rate"; } }
				public static string OneMinRate { get { return "_1 Min Rate"; } }
				public static string FiveMinRate { get { return "_5 Min Rate"; } }
				public static string FifteenMinRate { get { return "_15 Min Rate"; } }
			}
		}
	}

	/// <summary>
	/// The default <see cref="InfluxdbConverter"/> implementation which is simply a concrete type that derives from
	/// the abstract base class and provides no additional implementation on top of the base class implementation.
	/// </summary>
	public class DefaultConverter : InfluxdbConverter
	{
		/// <summary>
		/// Creates a new <see cref="DefaultConverter"/> using the specified tags.
		/// </summary>
		/// <param name="globalTags">The global tags that are added to all created <see cref="InfluxRecord"/> instances.</param>
		public DefaultConverter(MetricTags? globalTags = null)
			: base(globalTags)
		{
		}
	}
}
