/**
 * Created by eli on 9/9/2016.
 */

/**
 * Created by eli on 8/29/2016.
 */

$(document).ready(function () {
    city_autocomplete();
});

$(function () {
    $('#city_select').change(function () {
        city_autocomplete();
    });
});


function city_autocomplete() {  //TODO change to autocomplete instead of regular dropdown
    $.getJSON($SCRIPT_ROOT + '/_show_stores',
        {
            city: $('#city_select').find(":selected").text()
        },
        function (data) {
            var html = "";
            for (var i = 0; i < data.length; i++) {
                html += '<label for="' + data[i][0] + '">' + data[i][1] + ' - ' + data[i][2] + '</label>' +
                    '<input type="checkbox" name="store" id="' + data[i][0] + '"><br>';
            }
            $('#stores_form').html(html);
        }
    );
    return false;
}


$(function item_autocomplete() {
    $('#search').autocomplete({
            autoFocus: true,

            source: function (request, response) {
                var term = request.term,
                    key_splitter = "XXX",
                    selected_stores = get_selected_stores_ids().toString(),
                    element = this.element,
                    cache = element.data('autocompleteCache') || {},
                    foundInCache = false;

                $.each(cache, function (key, data) {
                    var key_term = key.split(key_splitter)[0],
                        stores = key.split(key_splitter)[1];
                    if (term.indexOf(key_term) === 0 && stores == selected_stores && data.items.length > 0) {
                        response(data.items);
                        foundInCache = true;
                        return;
                    }
                });
                if (foundInCache) return;

                $.getJSON($SCRIPT_ROOT + '/_search',
                    {
                        search: request.term,
                        stores_ids: get_selected_stores_ids()
                    },
                    function (data) {
                        cache[term + key_splitter + selected_stores.toString()] = data;
                        element.data('autocompleteCache', cache);
                        response(data.items);
                    }
                );
            },
            minLength: 3,  //TODO after how many characters will start searching
            select: function (event, ui) {
                event.preventDefault(); //disable the updating of search box with value instead of label
                $.getJSON($SCRIPT_ROOT + '/_get_item_history',
                    {
                        item_id: ui.item.value,
                        stores_ids: get_selected_stores_ids()
                    },
                    function (response) {
                        /* response.products is:
                         {
                         'name': item.name
                         'price_history': list of (price, date)
                         'store_id':
                         }
                         */
                        console.log(response.products_history[0]['price_history'][0]);
                        chart_history(response.products_history);
                    });
            },
            focus: function (event, ui) {
                event.preventDefault(); //disable the updating of search box with value instead of label
                $('search').val(ui.item.label);
            }

        }
    );
});

function chart_history(products) {
    // parse response
    var datasets = [];
    for (var i = 0; i < products.length; i++) {
        var data = [];
        var price_history = products[i]['price_history'];
        for (var j = 0; j < price_history.length; j++) {
            data.push({
                x: new Date(price_history[j][0]),
                y: price_history[j][1]
            });
        }
        datasets.push({
            label: products[i]['name'] + products[i]['store_id'],
            data: data,
            fill: false,
            showLine: true,
            pointRadius: 5,
            borderColor: getRandomColor(),
            lineTension: 0,
        });
    }
    var ctx = $("#myChart");
    var c = new Chart(ctx, {
        type: 'line',
        data: {datasets: datasets},
        options: {
            scales: {
                xAxes: [{
                    type: 'time',
                    position: 'bottom',
                    displayFormats: {
                        'millisecond': 'MMM DD',
                        'second': 'MMM DD',
                        'minute': 'MMM DD',
                        'hour': 'MMM DD',
                        'day': 'MMM DD',
                        'week': 'MMM DD',
                        'month': 'MMM DD',
                        'quarter': 'MMM DD',
                        'year': 'MMM DD',
                    }
                }],
                yAxes: [{
                    display: true,
                    ticks: {
                        suggestedMin: 0,    // minimum will be 0, unless there is a lower value.
                    }
                }]
            }
        }
    });

}


function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++ ) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}

function get_selected_stores_ids() {
    var ids = [];
    $('input:checkbox[name="store"]:checked').each(function () {
        ids.push(parseInt($(this).attr('id')));
    });
    return ids;
}
