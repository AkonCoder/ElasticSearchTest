using System;
using System.Linq;
using System.Net.Http.Formatting;
using System.Web.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace Script.I200.ElasticSearch
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            // 移除XML格式输出
            var json = config.Formatters.JsonFormatter;
            // 解决json序列化时的循环引用问题
            json.SerializerSettings.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;

            // 包括 exception detail
            config.IncludeErrorDetailPolicy = IncludeErrorDetailPolicy.Always;

            // 禁用XML序列化器
            config.Formatters.XmlFormatter.UseXmlSerializer = false;

            // 使用attribute路由规则 
            config.MapHttpAttributeRoutes();

            //日期格式
            config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new MyDateTimeConvertor());

            ////设置json对象的首字符小写
            var jsonFormatter = config.Formatters.OfType<JsonMediaTypeFormatter>().First();
            jsonFormatter.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
        }
    }

    /// <summary>
    /// 自定义时间格式
    /// </summary>
    public class MyDateTimeConvertor : IsoDateTimeConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"));
        }
    }
}
