frappe.ui.form.on("Purchase Order Schedule", {
    onload(frm) {
        frm.set_query('item_code', function() {
            return {
                query: "onegene.onegene.doctype.purchase_order_schedule.purchase_order_schedule.get_query_for_item_table",
                filters: {
                    purchase_order: frm.doc.purchase_order_number
                }
            };
        });
        toggle_qty_read_only(frm);
        if (frm.doc.amended_from && frm.doc.__islocal) {
            frm.set_value("revised", 0)
            amendment(frm);
        }
    },
    
    refresh(frm) {
        toggle_qty_read_only(frm);
        if (frm.doc.docstatus == 1 && !(frm.doc.pending_qty == 0) && frappe.user.has_role("Supplier")) {
            if (frappe.user.has_role("System Manager")) {
                toggle_revise_button(frm);
            }
        }
    },
    setup(frm) {
        toggle_qty_read_only(frm);
    },
    qty(frm) {
        const qty = frm.doc.qty || 0;
        const received_qty = frm.doc.received_qty || 0;
        frm.set_value('pending_qty', qty - received_qty)
    }
});

function toggle_qty_read_only(frm) {
    if (frm.doc.docstatus == 1 || frm.doc.docstatus == 2) {
        frm.fields_dict['qty'].df.read_only = 1;
    } else {
        frm.fields_dict['qty'].df.read_only = 0;
    }
    frm.refresh_field('qty');
    if (frm.doc.amended_from) {
        frm.fields_dict['purchase_order_number'].df.read_only = 1;
        frm.fields_dict['item_code'].df.read_only = 1;
        frm.fields_dict['schedule_date'].df.read_only = 1;
        frm.fields_dict['order_rate'].df.read_only = 1;
    }
    else {
        frm.fields_dict['purchase_order_number'].df.read_only = 0;
        frm.fields_dict['item_code'].df.read_only = 0;
        // frm.fields_dict['schedule_date'].df.read_only = 0;
        // frm.fields_dict['order_rate'].df.read_only = 0;
    }
}

function amendment(frm) {
        let dialog = new frappe.ui.Dialog({
            title: __('Revise Schedule Quantity'),
            fields: [
                {
                    label: __('Remarks'),
                    fieldname: 'remarks',
                    fieldtype: 'Small Text',
                    reqd: 1,
                    placeholder: __('Enter revision remarks')
                },
                {
                    label: __('Schedule Qty'),
                    fieldname: 'revision_qty',
                    fieldtype: 'Float',
                    reqd: 1,
                    default: frm.doc.qty,
                },
                {
                    label: __('Received Qty'),
                    fieldname: 'received_qty',
                    fieldtype: 'Float',
                    read_only: 1,
                    default: frm.doc.received_qty,
                }
            ],
            primary_action_label: __('Revise'),
            primary_action: function() {
                let values = dialog.get_values();
                if (values.remarks && values.revision_qty) {
                    if (values.revision_qty >= frm.doc.received_qty) {
                        frm.add_child("revision", {
                            'revised_on': frappe.datetime.now_datetime(),
                            'remarks': values.remarks,
                            'schedule_qty': frm.doc.qty,
                            'revised_schedule_qty': values.revision_qty,
                            'revised_by': frappe.session.user,
                        });
                        frm.refresh_field('revision');
                        frm.set_value('revised', 1);
                        frm.set_value("qty", values.revision_qty);
                        frm.set_value("disable_update_items", 0);
                        frm.save();
                        dialog.hide();
                    } else {
                        frappe.msgprint("Cannot set Schedule Quantity less than Received Quantity");
                    }
                }
            },
        });
        dialog.show();
}

function toggle_revise_button(frm) {
    frm.add_custom_button(__('Revise'), function() {
        let dialog = new frappe.ui.Dialog({
            title: __('Revise Schedule Quantity'),
            fields: [
                {
                    label: __('Remarks'),
                    fieldname: 'remarks',
                    fieldtype: 'Small Text',
                    reqd: 1,
                    placeholder: __('Enter revision remarks')
                },
                {
                    label: __('Schedule Qty'),
                    fieldname: 'revision_qty',
                    fieldtype: 'Float',
                    reqd: 1,
                    default: frm.doc.qty,
                },
                {
                    label: __('Received Qty'),
                    fieldname: 'received_qty',
                    fieldtype: 'Float',
                    read_only: 1,
                    default: frm.doc.received_qty,
                }
            ],
            primary_action_label: __('Revise'),
            primary_action: function() {
                let values = dialog.get_values();
                if (values.remarks && values.revision_qty) {
                    if (values.revision_qty >= frm.doc.received_qty) {
                        frappe.call({
                            method: "onegene.onegene.doctype.purchase_order_schedule.purchase_order_schedule.revise_schedule_qty",
                            args: {
                                name: frm.doc.name,
                                revised_qty: values.revision_qty,
                                remarks: values.remarks
                            },
                            freeze: true,
                            freeze_message: "Revising Schedule Quantity...",
                            callback: function () {
                                dialog.hide();
                            }
                        });
                    }
                    // else if (values.revision_qty == frm.doc.received_qty) {
                    //     frappe.msgprint("Schedule Quantity should be greater than Received Quantity");
                    // }
                    else {
                        frappe.msgprint("Cannot set Schedule Quantity less than Received Quantity");
                    }
                }
            },
        });
        dialog.show();
    }).addClass('btn-danger');
}
