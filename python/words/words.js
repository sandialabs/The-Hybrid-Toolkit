/*globals d3 */

window.onload = function () {
    "use strict";

    var w = 10, h = 80,
        data = [],
        chart,
        x,
        y,
        color,
        initlength = 1000,
        added;

    x = d3.scale.linear()
        .domain([0, 1])
        .range([0, w]);

    y = d3.scale.linear()
        .domain([-0.1, 1.1])
        .range([0, h]);

    color = d3.scale.linear()
        .domain([0, 0.5, 1])
        .range(['darkred', 'lightgray', 'steelblue']);

    chart = d3.select("body").append("svg")
        .attr("class", "chart")
        .attr("width", initlength - 1)
        .attr("height", h);

    chart.append("line")
        .attr("x1", 0)
        .attr("x2", w * initlength)
        .attr("y1", h / 2 - 0.5)
        .attr("y2", h / 2 - 0.5)
        .style("stroke", "#000");

    function redraw() {

        var rect = chart.selectAll("circle")
            .data(data, function (d) { return d._id["$oid"]; });

        console.log(rect.enter().length);

        rect.enter().insert("circle", "line")
            .attr("cx", function (d, i) { return x(i + added); })
            .attr("cy", function (d) { return h - y(d.vowels.fraction); })
            .attr("r", 3)
            .style("fill", "green")
            .style("stroke", "white")
            .style("stroke-width", 1)
            .append("title")
            .text(function (d) { return d.word; });

        rect.transition()
            .duration(1000)
            .attr("cx", function(d, i) { d.x = x(i); return x(i); });

        rect.exit().transition()
            .duration(1000)
            .attr("cx", function(d, i) { return d.x - x(added); })
            .remove();
    }

    function refresh() {
        d3.json('/service/mongo/localhost/local/test?query={"$and": [{"word":{"$exists":true}},{"vowels.fraction":{"$exists":true}}]}&limit=100&sort=[["timestamp",-1]]', function (error, d) {
            var idMap = {};
            data.forEach(function (d) {
                idMap[d._id["$oid"]] = true;
            });
            added = 0;
            d.result.data.forEach(function (d) {
                if (!idMap[d._id["$oid"]]) {
                    added += 1;
                }
            });
            data = d.result.data.reverse();
            redraw();
        });
    }

    refresh();
    window.setInterval(refresh, 5000);
};
