// Copyright (c) 2025, TEAMPRO and contributors
// For license information, please see license.txt

frappe.ui.form.on("Report Production Plan", {
	onload(frm) {
        frm.call('get_data').then(r=>{
			if (r.message) {
				frm.fields_dict.html.$wrapper.empty().append(r.message)
			}
	
		})	
	},
	generate_report(frm) {
        frm.call('get_data').then(r=>{
			if (r.message) {
				frm.fields_dict.html.$wrapper.empty().append(r.message)
			}
	
		})	
	},
	get_template_for_plan_qty: function (frm) {
        window.location.href = repl(frappe.request.url +
            '?cmd=%(cmd)s', {
            cmd: "onegene.onegene.doctype.report_production_plan.report_production_plan.get_template",
           
        })
	},
	create_plan_qty(frm){
		frappe.call({
			method: "onegene.onegene.doctype.report_production_plan.report_production_plan.enqueue_create_attendance",
			args: {
                attach :frm.doc.upload_plan_qty
			},
			freeze: true,
			freeze_message: 'Updating Qty....',
			callback: function (r) {
				if (r.message == "OK") {
					frappe.msgprint("Plan Qty Updated Successfully")
				}
			}
		});
	}	
});
