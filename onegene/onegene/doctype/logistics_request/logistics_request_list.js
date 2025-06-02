// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// License: GNU General Public License v3. See license.txt

// render
frappe.listview_settings["Logistics Request"] = {
    get_indicator: function (doc) {
        const status_colors = {
            Draft: "gray",
            Scheduled: "yellow",
            "Variation - Pending for Finance": "red",
            Dispatched: "orange",
            "In Transit": "blue",
            Delivered: "green",
            Closed: "red",
        };

        return [__(doc.status), status_colors[doc.status], "status,=," + doc.status];
    },
};
