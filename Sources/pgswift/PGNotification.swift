//
//  PGNotification.swift
//  pgswift
//
//  Created by Mark Lilback on 8/28/19.
//

import Foundation
import CLibpq

/// encapsulates a notification from the server
public struct PGNotification {
	/// processor ID of the notifying server
	public let pid: Int
	/// the notification channel name
	public let channel: String
	/// notification payload string
	public let payload: String?
	
	/// internal initializer
	init(pgNotify: PGnotify) {
		channel = String(cString: pgNotify.relname)
		pid = Int(pgNotify.be_pid)
		var proposedPayload: String? = nil
		if pgNotify.extra != nil {
			let string = String(cString: pgNotify.extra)
			if !string.isEmpty {
				proposedPayload = string
			}
		}
		payload = proposedPayload
	}
}
