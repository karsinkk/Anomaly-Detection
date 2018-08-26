
mapboxgl.accessToken = '';
var v1 = new mapboxgl.LngLat(134.472656,-24.149260);

var map = new mapboxgl.Map({
container: 'map',
center: v1,
style: 'mapbox://styles/mapbox/dark-v9',
zoom :2

});
var id = 0
var count = 0
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function removeid() {
while(count +10 < id) {
    map.removeLayer(count.toString());
  await sleep(1000);
  count = count+1
}
}
map.on('load', function() {

console.log("in map");
var socket = new WebSocket("ws://localhost:4000/data");
socket.onopen = function (event) {
  console.log("CONNECTED!!")
};
socket.onmessage = function (event) {
    removeid();
    console.log("in map");
  console.log(event.data);
  console.log(id)
    data = JSON.parse(event.data)
    updateTable(id,data);
    id = id+1
    if(id ==42){
        var v2 = new mapboxgl.LngLat(151.059265,-33.877577);
        map.setCenter(v2)
        map.zoomTo(9)
        
    }
    var color = "#007cbf"
    if(data.IsAnomaly){
        color = "#ef0404"
    }
        map.addLayer({
        "id": id.toString(),
         "source": {
                "type": "geojson",
                "data": {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [parseFloat(data.Lat),parseFloat(data.Long)]
                        }
                    }]
                }
            },
        "type": "circle",
        "paint": {
            "circle-radius": 6,
            "circle-color": color
        }
    });
};

});

async function updateTable(id,data){
    $("#map_loader").remove()
    $("#data_loader").remove()
    var d = new Date(0);
    $("#Table_body").children("tr:first").remove()
    time = parseFloat(data.Time)/1000
    color = ""
    d.setUTCSeconds(time);
    sms = (parseFloat((data.Sms_in)) + parseFloat((data.Sms_out))).toPrecision(3)
    cell = (parseFloat((data.Call_in)) + parseFloat((data.Call_out))).toPrecision(3)
    if(data.IsAnomaly){
        color = "#f44336"
    }
    $('#Table').append('<tr bgcolor='+color+'><td>'+id+'</td><td>'+d.getHours()+''+':'+''+d.getMinutes()+'</td><td>'+sms+'</td><td>'+cell+'</td><td>'+parseFloat((data.Internet)).toPrecision(3)+'</td></tr>')
}
