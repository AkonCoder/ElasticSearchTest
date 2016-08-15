using System.Web.Http;
using Nest;

namespace Script.I200.ElasticSearch.Controllers
{
    /// <summary>
    ///     Nest 客户端调用ES api接口测试
    /// </summary>
    [RoutePrefix("v0")]
    public class HomeController : BaseApiController
    {
        /// <summary>
        ///     创建ES客户端
        /// </summary>
        private ElasticClient _client;

        public HomeController()
        {
            _client = NestDemos.CreateClient();
        }
    }
}