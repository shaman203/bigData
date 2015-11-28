var mapFun = function() {
    //this.likes.forEach(function(like){ emit(like.blog, 1.0); });
    
    nr = this.likes.length;
    var emitedPairs = {};
    if(nr > 1)//we can emit liked blog pairs
    {
       for(var i = 0; i < nr -1; i++)
       {
           for(var j = i+1; j < nr; j++)
           {
               var blog1 = this.likes[i].blog;
               var blog2 = this.likes[j].blog;
               if(!(blog1 === blog2))
               {
                   var newKey = {v1:"kacsa", v2:"macska"}; 
                   if(blog1 > blog2)
                   {
                       newKey = {v1:blog2, v2:blog1};
                   }
                   else
                   {
                       newKey = {v1:blog1, v2:blog1};
                   }
                   if(typeof emitedPairs[newKey.v1] === 'undefined'){
                        emitedPairs[newKey.v1] = {};
                   };
                   if(typeof emitedPairs[newKey.v1][newKey.v2] === 'undefined')
                   {
                       emitedPairs[newKey.v1][newKey.v2] = 1;
                       emit(newKey, 1);         
                   }
                }
           }
        }
    }
}
var reduceFun = function(blogName, likeCount) {
                        var sum = 0.0;
                        for( var i = 0; i < likeCount.length; i++)
                        {
                           sum += likeCount[i];
                        }
       
                       return sum;
                      };
    
db.getCollection('clusters2').mapReduce(
                     mapFun,
                     reduceFun,
                     { out: "mrResult" }
                   )    