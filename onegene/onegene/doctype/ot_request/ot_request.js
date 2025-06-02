// Copyright (c) 2024, TEAMPRO and contributors
// For license information, please see license.txt

frappe.ui.form.on("OT Request", {
    onload(frm){
        frm.fields_dict.employee_details.grid.get_field('employee_code').get_query =function() {
            return {
            filters: {
                "employee_category": frm.doc.employee_category,
                "department": frm.doc.department,
                "status":'Active',
            }
            }
        }
        
    },
	refresh(frm){
        frm.set_query("employee_category", function (){
            return {
                filters: {
                    "name": ["not in", ["Staff","Sub Staff"]]
                }
            }
        }) 
        
        
        
        frappe.db.get_value("Company",{},'name')
        .then(r => {
            console.log(r.message.name)
            frm.set_value('company',r.message.name)
        })
    },
    after_insert(frm){

    },
    validate(frm) {
        // frm.doc.employee_details.forEach((e) => {
        //     frappe.call({
        //         method: "frappe.client.get",
        //         args: {
        //             doctype : "Employee",
        //             filters:{
        //                 'name':e.employee_code
        //             },
        //             fields:['employee_category']
        //         },
        //         callback: function(r) {
                    
        //             if (["Staff", "Sub Staff"].includes(r.message.employee_category)) {
        //                 frappe.validated=false
        //                 frappe.msgprint("Row" +e.idx+ ":Employee is in "+r.message.employee_category + " category. Kindly remove that row and save.")
        //             }
        //             else{
        //                 frappe.validated=true
        //             }
        //         }
        //     })
        // })
        // frm.doc.employee_details.forEach((e) => {
        // frappe.call({
        //     method: "frappe.client.get",
        //     args: {
        //         doctype : "Employee",
        //         filters:{
        //             'name':e.employee_code
        //         },
        //         fields:['department']
        //     },
        //     callback: function(r) {
                
        //         if (r.message.department!=frm.doc.department) {
        //             frappe.validated=false
        //             frappe.msgprint("Row" +e.idx+ ":Employee is in "+r.message.department + " department.So it was removed.")
        //             // frappe.model.clear_doc(child.doctype, child.name); // Removes the row
        //             // cur_frm.refresh_field("employee_details");
        //         }
        //         else{
        //             frappe.validated=true
        //         }
        //     }
        // })
    // })
        current_date=frappe.datetime.nowdate()
        // console.log(current_date)
        // console.log(frm.doc.ot_requested_date)
        if(frm.doc.ot_requested_date<current_date){
            // if (frappe.session.user!='senthilkumar.r@onegeneindia.in'){
            //     frappe.throw("Not allowed to apply OT Request for the Past Date")
            // }
            }
        let total_ot = 0;
        frm.doc.employee_details.forEach((detail) => {
            if (detail.requested_ot_hours) {
                total_ot += parseInt(detail.requested_ot_hours, 10); 
            }
        });
        frm.set_value('ot_hours', total_ot);
	},
    employee_category(frm){
        frm.clear_table("employee_details");
        frm.trigger("department")
    },
    department(frm){
        if(frm.doc.department){
            frm.clear_table("employee_details");
            frappe.call({
                method: "onegene.onegene.doctype.ot_request.ot_request.get_employees",
                args: {
                    dept : frm.doc.department,
                    category:frm.doc.employee_category
                },
                callback: function(r) {
                    
                    if (r.message) {
                        $.each(r.message, function(i, d) {
                            let emp = frm.add_child("employee_details");
                            emp.employee_code = d.name;  
                            emp.employee_name = d.employee_name;
                            emp.designation = d.designation;
                    
                        });
                    
                        frm.refresh_field('employee_details');
                    }
                    
                    
                    else{
                        frm.clear_table("employee_details");
                    }

                    
                }
            })
        }
    },
    ot_requested_date(frm){
        current_date=frappe.datetime.nowdate()
        // if(frm.doc.ot_requested_date<current_date){
        //     frappe.throw("Not allowed to apply OT Request for the Past Date")
        // }
    },
   
});

frappe.ui.form.on('OT Request Child', {
    employee_code: function(frm, cdt, cdn) {
        const child = locals[cdt][cdn]; 
        const employee_code = child.employee_code;
        let isDuplicate = false;
        $.each(frm.doc.employee_details || [], function(i, row) {
            if (row.employee_code === employee_code && row.name !== child.name) {
                isDuplicate = true;
                return false; 
            }
        });
        if (isDuplicate) {
            frappe.msgprint('This employee code already exists in the table.');
            frappe.model.remove_from_locals(cdt, cdn);
            frm.refresh_field('employee_details');
        }
        frappe.call({
            method: "onegene.onegene.doctype.ot_request.ot_request.get_details",
            args: {
                'name': child.employee_code,
                'dep': frm.doc.department
            },
            callback(r) {
                if (r.message){
                    console.log(r.message)
                    if(r.message=='OK'){
                        // frappe.model.set_value(cdt,cdn,'employee_name','');
                        // frappe.model.set_value(cdt,cdn,'designation','');
                        // frappe.model.set_value(cdt,cdn,'employee_code','');
                        frappe.msgprint("Row" +child.idx+ ":Employee is in Staff " + " category.So it was removed.")
                        frappe.model.clear_doc(child.doctype, child.name); // Removes the row
                        cur_frm.refresh_field("employee_details");
                    }
                    else if(r.message=='ok'){
                        // frappe.model.set_value(cdt,cdn,'employee_name','');
                        // frappe.model.set_value(cdt,cdn,'designation','');
                        // frappe.model.set_value(cdt,cdn,'employee_code','');
                        frappe.msgprint("Row" +child.idx+ ":Employee is from other " + " department.So it was removed.")
                        frappe.model.clear_doc(child.doctype, child.name); // Removes the row
                        cur_frm.refresh_field("employee_details");
                    }
                    else{
                        const [employee_name, designation] = r.message; 
                        frappe.model.set_value(cdt,cdn,'employee_name',employee_name);
                        frappe.model.set_value(cdt,cdn,'designation',designation);
                    }
                }
                
            }
        });
        // frappe.call({
        //     method: "frappe.client.get",
        //     args: {
        //         doctype : "Employee",
        //         filters:{
        //             'name':child.employee_code
        //         },
        //         fields:['employee_category']
        //     },
        //     callback: function(r) {
                
        //         if (["Staff", "Sub Staff"].includes(r.message.employee_category)) {
        //             frappe.msgprint("Row" +child.idx+ ":Employee is in "+r.message.employee_category + " category.So it was removed.")
        //             frappe.model.clear_doc(child.doctype, child.name); // Removes the row
        //             cur_frm.refresh_field("employee_details");
        //         }
        //     }
        // })
        // frappe.call({
        //     method: "frappe.client.get",
        //     args: {
        //         doctype : "Employee",
        //         filters:{
        //             'name':child.employee_code
        //         },
        //         fields:['department']
        //     },
        //     callback: function(r) {
                
        //         if (r.message.department!=frm.doc.department) {
        //             frappe.msgprint("Row" +child.idx+ ":Employee is in "+r.message.department + " department.So it was removed.")
        //             frappe.model.clear_doc(child.doctype, child.name); // Removes the row
        //             cur_frm.refresh_field("employee_details");
        //         }
        //     }
        // })
        
    },
    requested_ot_hours: function(frm, cdt, cdn) {
        const child = locals[cdt][cdn]; 
        if (child.requested_ot_hours){     
        if (child.requested_ot_hours < 2) {
            frappe.model.set_value(cdt,cdn,'requested_ot_hours','');
            frappe.throw("Minimum time allowed for request is 2 hours. Below 2 are not applicable.");
        }
        var regex = /^[0-9]+$/;
		if (!regex.test(child.requested_ot_hours) === true) {
		    console.log(regex)
            frappe.model.set_value(cdt,cdn,'requested_ot_hours','');
			frappe.throw(__("Only Interger Values are allowed"));
			
		}
    }
    }
});

