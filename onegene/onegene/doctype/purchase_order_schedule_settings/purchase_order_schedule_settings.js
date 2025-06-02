// Copyright (c) 2025, TEAMPRO and contributors
// For license information, please see license.txt

frappe.ui.form.on('Purchase Order Schedule Settings', {
	refresh(frm) {
        frm.fields_dict.html_1.$wrapper.html(`
            <p style="font-size: 15px;">A) Download the template of purchase order schedule</p>
        `);
        frm.fields_dict.html_2.$wrapper.html(`
            <p style="font-size: 15px; margin-top: 45px;">B) Attach the file to be uploaded</p>
        `);
        frm.fields_dict.html_3.$wrapper.html(`
            <p style="font-size: 15px; margin-top: 45px;">C) Upload the attached file</p>
        `);
        frm.fields_dict.html_4.$wrapper.html(`
            <p style="min-height: 15px; max-height: 15px"></p>
        `);
        frm.disable_save()
        if(frm.doc.attach) {
            frappe.call({
                method: 'onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.get_data',
                args: {
                    'file': frm.doc.attach,
                },
                callback(r) {
                    if (r.message) {
        				frm.fields_dict.html.$wrapper.empty().append(r.message)

                    }
                }
            })
            
        }
        frappe.realtime.on('purchase_order_upload_progress', function(data) {
           
            if (!frappe.upload_dialog) {
                frappe.upload_dialog = new frappe.ui.Dialog({
                    title: __('Uploading'),
                    indicator: 'orange',
                    fields: [
                        {
                            fieldtype: 'HTML',
                            fieldname: 'progress_html'
                        }
                    ],
                    
                    static: true
                });
        
                frappe.upload_dialog.show();
                frappe.upload_dialog.$wrapper.find('.modal').modal({
                    backdrop: 'static',  
                    keyboard: false     
                });
            }
        
            let percent = Math.round(data.progress);
            frappe.upload_dialog.set_title(__(data.stage || 'Creating Purchase Order Schedule'));
            frappe.upload_dialog.get_field('progress_html').$wrapper.html(`
                <div class="progress" style="height: 20px;">
                    <div class="progress-bar progress-bar-striped progress-bar-animated bg-success" 
                         role="progressbar" style="width: ${percent}%">
                        ${percent}%
                    </div>
                </div>
                <p style="margin-top: 10px;">${__(data.description || '')}</p>
            `);
        
            if (percent >= 100 && data.stage === 'Updating Purchase Order') {
                setTimeout(() => {
                    frappe.upload_dialog.hide();
                    frappe.upload_dialog = null;
                }, 2000);
            }

            // if (percent >= 100 && data.stage === 'Updating Purchase Order') {
            //     setTimeout(() => {
            //         frappe.upload_dialog.hide();
            //         frappe.upload_dialog = null;
            //     }, 2000);
            // }
        });
	},
    
    
    upload(frm, done = null) {
        if (frm.doc.attach) {
            frappe.call({
                method: 'onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.enqueue_upload',
                args: {
                    file: frm.doc.attach
                },
                // freeze: true,
                // freeze_message: __('Updating Order Schedule Data...')
            }).then((r) => {
                let result = r.message;
    
                if (result) {
                        frappe.msgprint({
                            message: __('Uploaded Successfully'),
                            indicator: 'orange'
                        });
                        if (done) done();
                }
            })
        } else {
            frappe.msgprint(__('Please attach the Excel file before uploading.'));
        }
    },
    
    
    
    download(frm){
        var path = "onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.template_sheet"
		var args = 'name=%(name)s'
		if (path) {
			window.location.href = repl(frappe.request.url +
				'?cmd=%(cmd)s&%(args)s', {
				cmd: path,
				args: args,
				name: frm.doc.name
			});
		}
    },
})

