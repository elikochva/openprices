/**
 * Created by eli on 8/29/2016.
 */

$(function () {
        $('#city_select').change(function () {
                $.getJSON($SCRIPT_ROOT + '/_show_stores',
                    {
                        city: $('#city_select').find(":selected").text()
                    },
                    function (data) {
                        var html = "";
                        for (var i = 0; i < data.length; i++) {
                            html += '<input type="checkbox" name="store" id="' + data[i][0] + '">' + data[i][1] + '-' + data[i][2] + '<br>';
                        }
                        $('#stores_form').html(html);
                    }
                );
                return false;
            }
        );
    }
);

$(function () {
    $('#search').autocomplete({
            autoFocus: true,

            source: function (request, response) {

                $.getJSON($SCRIPT_ROOT + '/_search',
                    {
                        search: request.term,
                        store_ids: get_store_ids()
                    },
                    function (data) {
                        response(data.items);
                    }
                );
            },
            minLength: 3,  //TODO after how many characters will start searching

            select: function (event, ui) {
                event.preventDefault(); //disable the updating of search box with value instead of label
                $('search').val(ui.item.label);
                productList.add({
                    name: ui.item.label,
                    value: ui.item.value
                });
            },
            focus: function (event, ui) {
                event.preventDefault(); //disable the updating of search box with value instead of label
                $('search').val(ui.item.label);
            }

        }
    );
});

$(function () {
    productList.on('updated', function () {
        $.getJSON($SCRIPT_ROOT + '/_total_price',
            {
                product_ids: get_product_ids()
            },
            function (data) {

                return data.totals;
            }
        );
        var totalPrices = [];
        for
        totalPrices.push(total);
    });
    $('#total').val(totalPrice[0]);
});

function get_store_ids() {
    var ids = [];
    $('input:checkbox[name="store"]:checked').each(function () {
        ids.push(parseInt($(this).attr('id')));
    });
    return ids;
}

function get_product_ids() {
    var ids = [];
    $('#basket:ul:li:h5[class="value"]').each(function () {
        ids.push(parseInt($(this).attr('value')));
    });
    return ids;
}