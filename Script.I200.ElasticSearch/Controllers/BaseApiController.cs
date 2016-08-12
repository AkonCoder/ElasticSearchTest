using System.Web;
using System.Web.Http;

namespace Script.I200.ElasticSearch.Controllers
{
    public class BaseApiController : ApiController
    {
        public string GetStringRequest(string paramter)
        {
            return HttpContext.Current.Request.QueryString[paramter] ?? "";
        }

        public int? GetIntRequest(string paramter)
        {
            var tmp = HttpContext.Current.Request.QueryString[paramter] ?? "";
            var tag = 0;
            if (!int.TryParse(tmp, out tag))
            {
                return null;
            }
            return tag;
        }
    }
}