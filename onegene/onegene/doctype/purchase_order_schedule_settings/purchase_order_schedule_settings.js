// Copyright (c) 2025, TEAMPRO and contributors
// For license information, please see license.txt

frappe.ui.form.on('Purchase Order Schedule Settings', {
    refresh(frm) {
        
        frm.fields_dict.response.$wrapper.html(frm.doc.response_data);

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

        const fullMonthNames = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ];

        const shortMonthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

        let date = new Date();
        let fullMonth = fullMonthNames[date.getMonth()];
        let shortMonth = shortMonthNames[date.getMonth()];

        frm.fields_dict.note.$wrapper.html(`
            <p style="min-height: 15px; max-height: 15px; color: red; margin-bottom: 30px;">
                <b>Note: </b>If the Schedule Period is for <b>${fullMonth}</b>, the value should be in the format <b>${shortMonth.toUpperCase()}</b> or <b>${shortMonth}</b>.
            </p>
        `);

        frm.disable_save()
        if (frm.doc.attach) {
            frappe.call({
                method: 'onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.get_data',
                args: {
                    'file': frm.doc.attach,
                },
                callback(r) {
                    if (r.message) {
                        frm.fields_dict.html.$wrapper.empty().append(r.message)
                        frm.set_df_property('upload', 'hidden', 0)

                    }
                }
            })

        }
        else {
            frm.fields_dict.html.$wrapper.empty()
        }
        frappe.realtime.on('purchase_order_upload_progress', function(data) {
            if (!frappe.upload_dialog) {
                frappe.upload_dialog = new frappe.ui.Dialog({
                    title: __('Uploading'),
                    indicator: 'orange',
                    fields: [{
                        fieldtype: 'HTML',
                        fieldname: 'progress_html'
                    }],
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

            let description_html = data.description || '';

            if (data.errors && data.errors.length > 0) {
                description_html = `<span style="color:black; font-size: 12px;"><b>Errors encountered:</b><br>${data.errors.map(err => frappe.utils.escape_html(err)).join('<br>')}</span>`;

                frappe.upload_dialog.set_primary_action(__('Close'), () => {
                    frappe.upload_dialog.hide();
                    frappe.upload_dialog = null;
                });
            } else {
                frappe.upload_dialog.set_primary_action(null);
            }

            frappe.upload_dialog.get_field('progress_html').$wrapper.html(`
                <div class="progress" style="height: 20px;">
                    <div class="progress-bar progress-bar-striped progress-bar-animated bg-success" 
                        role="progressbar" style="width: ${percent}%">
                        ${percent}%
                    </div>
                </div>
                <p style="margin-top: 10px;">${__(description_html)}</p>
            `);

            if (
                percent >= 100 &&
                data.stage === 'Completed Successfully' &&
                !(data.errors && data.errors.length > 0)
            ) {
                setTimeout(() => {
                    frappe.upload_dialog.hide();
                    frappe.upload_dialog = null;
                }, 2000);
            }
        });

        frappe.realtime.on("po_upload_summary", (data) => {
            const response_data = `
            <div style="
                max-width: 420px;
                margin: 20px auto;
                background: #ffffff;
                border-radius: 12px;
                padding: 20px 24px;
                box-shadow: 0 4px 14px rgba(0,0,0,0.08);
                font-family: Inter, sans-serif;
                border: 1px solid #e7e7e7;
            ">
                <div style="font-size: 18px; font-weight: 600; color: #333;">
                    Last Updated On
                </div>

                <div style="margin-top: 6px; font-size: 13px; color: #666;">
                    ${getCurrentDateTime()}
                </div>

                <hr style="margin: 12px 0; border: 0; border-top: 1px solid #eee;">

                <ul style="margin: 0; padding-left: 20px; line-height: 1.8;">
                    <li style="color:#4caf50">New documents created: ${data.new}</li>
                    <li style="color:#2196f3">Updated in existing document: ${data.update}</li>
                    <li style="color:#999">Skipped documents: ${data.skip}</li>
                </ul>
            </div>
            `
            frm.fields_dict.response.$wrapper.html(response_data);
            frappe.msgprint(`
                <ul>
                    <li>New documents created: ${data.new}</li>
                    <li>Updated in existing document: ${data.update}</li>
                    <li>Skipped documents: ${data.skip}</li>
                </ul>
            `, "Upload Summary");
        });

    },

    attach(frm) {
        if (frm.doc.attach) {
            frappe.call({
                method: 'onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.enqueue_upload_validate_po',
                args: {
                    file: frm.doc.attach
                },
                async: false,
                callback: function(r) {
                    if (r.message) {
                        // Display the returned HTML inside your HTML field
                        frm.fields_dict.error_data.$wrapper.empty().append(r.message);
                        frm.fields_dict.html.$wrapper.empty();
                        frm.set_value('attach', null);
                        frm.set_df_property('upload', 'hidden', 1)
                    }
                },
            });
        }
        else {
            frm.set_value("response_data", "")
        }
    },
    upload(frm, done = null) {
        if (frm.doc.attach) {
            frappe.call({
                method: 'onegene.onegene.doctype.purchase_order_schedule_settings.purchase_order_schedule_settings.enqueue_upload',
                args: {
                    file: frm.doc.attach
                },
                freeze: true,
                // freeze_message: __('Updating Order Schedule Data...')
            }).then((r) => {
                let result = r.message;
                if (result) {
                    frm.set_value('attach', null);
                    
                    
                    // frappe.msgprint({
                    //     message: __('Uploaded Successfully'),
                    //     indicator: 'orange'
                    // });
                    if (done) done();
                    
                }
            })
        } else {
            frappe.msgprint(__('Please attach the Excel file before uploading.'));
        }
    },



    download(frm) {
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

function getCurrentDateTime() {
    const now = new Date();

    const dd = String(now.getDate()).padStart(2, '0');
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const yyyy = now.getFullYear();

    const hh = String(now.getHours()).padStart(2, '0');
    const min = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');

    return `${dd}-${mm}-${yyyy} ${hh}:${min}:${ss}`;
}