/**
 * Created by eli on 8/29/2016.
 */

$(document).on('click', '.remove-item-btn', function () {
    var tr = $(this).closest('tr');
    var item_id = tr.find('.item_id').text();
    //TODO update totals price
    var item_count = get_item_count(item_id); //TODO do we need funciton here?
    $.each(basket_stores_ids(), function (index, store_id) {
        var price = get_item_price(item_id, store_id);
        update_total_price(store_id, -(price * item_count));
    });
    tr.remove();
});


$(document).ready(function () {
    city_autocomplete();
    init_basket();
});

$(function () {
    $('#city_select').change(function () {
        city_autocomplete();
    });
});

$(function () {
    $('#stores_form').on('change', 'input:checkbox[name="store"]', (function () {
            var store_data = get_selected_store_id_name(this);
            if (this.checked) {
                add_basket_store(store_data['id'], store_data['name']);
            }
            else {
                remove_basket_store(store_data['id']);
            }
        })
    )
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
                $.getJSON($SCRIPT_ROOT + '/_add_item',
                    {
                        item_id: ui.item.value,
                        stores_ids: get_selected_stores_ids()
                    },
                    function (response) {
                        /* response.products is:
                         {
                         'id': (item_id)
                         'store_id':
                         'name':
                         'price':
                         }
                         */
                        add_item_to_basket(response.products);
                    });
            },
            focus: function (event, ui) {
                event.preventDefault(); //disable the updating of search box with value instead of label
                $('search').val(ui.item.label);
            }

        }
    );
});

function get_selected_stores_ids() {
    var ids = [];
    $('input:checkbox[name="store"]:checked').each(function () {
        ids.push(parseInt($(this).attr('id')));
    });
    return ids;
}

function get_selected_stores_names() {
    var names = [];
    $('input:checkbox[name="store"]:checked').each(function () {
        names.push($('label[for="' + this.id + '"]').text());
    });
    return names;
}

function get_selected_stores_ids_names() {
    var res = [];
    $('input:checkbox[name="store"]:checked').each(function () {
        res.push(get_selected_store_id_name(this));
    });
    return res;
}

function get_selected_store_id_name(checkbox) {
    return {
        'id': parseInt(checkbox.id),
        'name': $('label[for="' + checkbox.id + '"]').text()
    }
}

function add_item_to_basket(data) {
    var item_id = data[0]['id'],
        item_name = data[0]['name'];

    $('table').show();
    var tbody = $('#basket>>tbody');
    if (!tbody.length) {
        tbody = init_basket();
    }
    // loop over all active stores
    if (!is_basket_item_exists(item_id)) {
        var tr = $('<tr>');
        for (var i = 0; i < get_selected_stores_ids().length; i++) { //fill up row with empty cells
            tr.append('<td class="product_price"></td>');
        }

        tr.append('<td class="product_name">' + item_name + '</td>'); //need to have same col index as of product_names th
        //save item id for future adding to table,
        // need to have same col index as of item_ids th
        tr.append('<td class="product_counter">0</td>');
        tr.append('<td class="item_id" hidden="hidden">' + item_id + '</td>');
        tr.append('<td class="remove"><button class="remove-item-btn">הסר</button></td>'); //remove button
        //TODO add +- buttons for adding more of same item
        tbody.append(tr);
    }
    $.each(data, function (index, value) {
        var store_id = value['store_id'],
            price = value['price'];
        add_item_price_to_store_basket(store_id, price, item_id);
    });
    update_item_count(item_id, 1);
}

function is_basket_item_exists(item_id) {
    return get_basket_item_row(item_id).length > 0;
}

function get_basket_item_row(item_id) {
    // console.log(item_id + typeof item_id);
    var p = $('.item_id');
    return p.filter(function () {
        return $(this).text() === item_id;
    }).parent();
}

function add_item_price_to_store_basket(store_id, price, item_id) {
    var i = get_store_basket_col(store_id);
    var td = get_basket_item_row(item_id).find('td').eq(i);
    update_total_price(store_id, price);
    td.text(price); // TODO need to be done only once *except from promotions
}


function update_item_count(item_id, count) {// negative count for removing
    var counter = get_basket_item_row(item_id).find('.product_counter');
    counter.text(parseInt(counter.text()) + count);
}

function update_total_price(store_id, price) { // negative price for removing
    var total = $('#total_' + store_id);
    total.val(parseFloat(total.val()) + price);
}

function get_basket_table() {
    var basket_div = $('#basket');
    var table = basket_div.find('table');
    if (!table.length) {
        table = $('<table>');
        basket_div.append(table);
    }
    return table;
}

function get_basket_tbody() {
    return get_basket_table().find('tbody');
}

function init_basket() {
    //TODO check if table exists
    var table = get_basket_table();
    var thead = $('<thead>');
    var tbody = $('<tbody class="list">');
    var names_tr = $('<tr id="basket_names">');
    var totals_tr = $('<tr id="basket_totals">');
    names_tr.append('<th id="item_names" colspan="4">מוצרים</th>');
    names_tr.append('<th id="item_ids" hidden="hidden"></th>');
    thead.append(names_tr);
    thead.append(totals_tr);
    table.append(thead);
    table.append(tbody);
    table.hide();
}
function clear_basket() {

}

function basket_stores_ids() {
    var res = [];
    $.each($('input[id*="total_"]'), function () {
        res.push(parseInt($(this).attr('id').match(/\d+$/)[0]));
    });
    return res;
}

function add_basket_store(store_id, name) {
    var totals_tr = $('#basket_totals'),
        names_tr = $('#basket_names');


    names_tr.prepend("<th>" + name + "</th>");
    totals_tr.prepend('<th><input type="text" id="total_' + store_id + '" value="0" disabled="disabled"></th>');

    $.each($('.item_id'), function () {
        var item_id = this.innerText;
        //TODO logic for getting all items ids, count
        $.getJSON($SCRIPT_ROOT + '/_add_item',  //TODO use different function? (already have item id, and store id...
            {
                item_id: item_id,
                stores_ids: [store_id]
            },
            function (response) {
                /* response.products is:
                 {
                 'id': (item_id)
                 'store_id':
                 'name':
                 'price':
                 }
                 */
                var count = get_item_count(item_id),
                    td = $('<td>').prependTo(get_basket_item_row(item_id));
                var res = response.products[0],
                    price = parseFloat(res['price']);

                console.log(res);
                console.log(price + ' count: ' + count);
                update_total_price(store_id, price * count);
                td.text(price); // TODO need to be done only once *except from promotions
            });
    });
}

function remove_basket_store(store_id) {
    var i = get_store_basket_col(store_id) + 1;
    $('#basket th:nth-child(' + i + ')').remove();
    $('#basket td:nth-child(' + i + ')').remove();
    if (!get_selected_stores_ids().length) {
        clear_basket();
    }
}

function get_item_count(item_id) {
    return parseInt(get_basket_item_row(item_id).find('.product_counter').text());
}

function get_store_basket_col(store_id) { // 0 based indexing !!
    return $('#total_' + store_id).parent().index();
}

function get_item_price(item_id, store_id) {
    var col = get_store_basket_col(store_id) + 1;
    console.log(get_basket_item_row(item_id).find('td:nth-child(' + col + ')'));
    return parseFloat(get_basket_item_row(item_id).find('td:nth-child(' + col + ')').text());
}
