$(".customercentric").show();
$(".productcentric").hide()

$("#product-centric-tab").on("click", function () {
    $(".customercentric").hide();
    $(".productcentric").show()

})

$("#customer-centric-tab").on("click", function () {
    $(".customercentric").show();
    $(".productcentric").hide()

})


$('select').selectpicker();