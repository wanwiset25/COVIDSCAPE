function tryLogin(){
    var user = document.getElementById("stacked-email").value
    var password = document.getElementById("stacked-password").value
    alert(user+"\n"+password)
  }


function tryReport(){
    var msg = prompt("Please enter your username")
    if (msg != ""){
        const now = new Date();
        var timestamp = now.getTime();
        var xhr = new XMLHttpRequest();
        var URLstring = "http:\\\\52.53.216.170:5000/?userid="+msg+"&diagnosed=True&timestamp="+timestamp
        console.log(timestamp)
        console.log(URLstring)
        xhr.open("GET", URLstring)
        //xhr.withCredentials = true;
        xhr.onreadystatechange = function() {
        if (xhr.readyState == 4) {
          // JSON.parse does not evaluate the attacker's scripts.
      
          response = xhr.responseText
          console.log(response)
          document.getElementById("notified").innerHTML = response
       
          }
        }
        xhr.send();
      
      
      
    }
  }