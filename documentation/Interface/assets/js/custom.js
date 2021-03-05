$("customercentric").hide();
$("productcentric").hide()

console.log("sdfsdf", $(".customercentric"))
$("#product-centric-tab").on("click", function () {
    $(".customercentric").hide();
    $(".productcentric").show()

})

$("#customer-centric-tab").on("click", function () {
    $(".customercentric").show();
    $(".productcentric").hide()

})


